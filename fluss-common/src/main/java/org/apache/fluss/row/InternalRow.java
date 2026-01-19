/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.row.columnar.VectorizedColumnBatch;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.row.InternalArray.createDeepElementGetter;
import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;

/**
 * Base interface for an internal data structure representing data of {@link RowType}.
 *
 * <p>The mappings from SQL data types to the internal data structures are listed in the following
 * table:
 *
 * <pre>
 * +--------------------------------+-----------------------------------------+
 * | SQL Data Types                 | Internal Data Structures                |
 * +--------------------------------+-----------------------------------------+
 * | BOOLEAN                        | boolean                                 |
 * +--------------------------------+-----------------------------------------+
 * | CHAR / STRING                  | {@link BinaryString}                    |
 * +--------------------------------+-----------------------------------------+
 * | BINARY / BYTES                 | byte[]                                  |
 * +--------------------------------+-----------------------------------------+
 * | DECIMAL                        | {@link Decimal}                         |
 * +--------------------------------+-----------------------------------------+
 * | TINYINT                        | byte                                    |
 * +--------------------------------+-----------------------------------------+
 * | SMALLINT                       | short                                   |
 * +--------------------------------+-----------------------------------------+
 * | INT                            | int                                     |
 * +--------------------------------+-----------------------------------------+
 * | BIGINT                         | long                                    |
 * +--------------------------------+-----------------------------------------+
 * | FLOAT                          | float                                   |
 * +--------------------------------+-----------------------------------------+
 * | DOUBLE                         | double                                  |
 * +--------------------------------+-----------------------------------------+
 * | DATE                           | int (number of days since epoch)        |
 * +--------------------------------+-----------------------------------------+
 * | TIME                           | int (number of milliseconds of the day) |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITHOUT TIME ZONE    | {@link TimestampNtz}                    |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH LOCAL TIME ZONE | {@link TimestampLtz}                    |
 * +--------------------------------+-----------------------------------------+
 * | ARRAY                          | {@link InternalArray}                   |
 * +--------------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>Nullability is always handled by the container data structure.
 *
 * @since 0.1
 */
// InternalRow 是 Apache Fluss 内部非常关键的一个基础接口。它定义了系统中**行数据（Row）**的通用表现形式和访问规范。
// 统一数据模型：它为所有的 SQL 数据类型（如 INT, STRING, DECIMAL 等）与 Java 内部表示（如 int, BinaryString, Decimal 等）之间建立了一一对应的映射关系。
// 解耦物理存储：无论底层数据是存储在内存（如 GenericRow）、二进制字节（如 BinaryRow）还是列式存储（如 ColumnarRow）中，顶层逻辑都可以通过 InternalRow 接口以统一的方式读取。
// 高效访问：它继承了 DataGetters 接口，提供了一系列类型化的 Getter 方法（如 getInt, getString），避免了频繁的装箱和拆箱操作。

@PublicEvolving
public interface InternalRow extends DataGetters {

    /**
     * Returns the number of fields in this row.
     *
     * <p>The number does not include {@link ChangeType}. It is kept separately.
     */
    // 返回当前行中字段（列）的总数。
    int getFieldCount();

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /** Returns the data class for the given {@link DataType}. */
    // 根据传入的 DataType（逻辑数据类型），返回其对应的 Java 内部类。
    // 示例：传入 STRING 类型，返回 BinaryString.class；传入 BIGINT，返回 Long.class。
    static Class<?> getDataClass(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                return BinaryString.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case BYTES:
                return byte[].class;
            case DECIMAL:
                return Decimal.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampNtz.class;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampLtz.class;
            case ARRAY:
                return InternalArray.class;
            case MAP:
                return InternalMap.class;
            case ROW:
                return InternalRow.class;
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /**
     * Creates an array of accessors for getting fields in an internal row data structure.
     *
     * @param rowType the row type of the internal row
     */
    // 为一整行数据批量创建访问器数组。
    static FieldGetter[] createFieldGetters(RowType rowType) {
        final FieldGetter[] fieldGetters = new FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldGetters[i] = createFieldGetter(rowType.getTypeAt(i), i);
        }
        return fieldGetters;
    }

    /**
     * Creates an accessor for getting elements in an internal row data structure at the given
     * position.
     *
     * @param fieldType the element type of the row
     * @param fieldPos the element position of the row
     */
    // 为特定的字段创建**浅拷贝（Shallow）**访问器。
    static FieldGetter createFieldGetter(DataType fieldType, int fieldPos) {
        final FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
                final int bytesLength = getLength(fieldType);
                fieldGetter = row -> row.getChar(fieldPos, bytesLength);
                break;
            case STRING:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
                final int binaryLength = getLength(fieldType);
                fieldGetter = row -> row.getBinary(fieldPos, binaryLength);
                break;
            case BYTES:
                fieldGetter = row -> row.getBytes(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestampNtz(fieldPos, timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestampLtz(fieldPos, timestampLtzPrecision);
                break;
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
                final int numFields = ((RowType) fieldType).getFieldCount();
                fieldGetter = row -> row.getRow(fieldPos, numFields);
                break;
            default:
                throw new IllegalArgumentException("Illegal type: " + fieldType);
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /**
     * Creates a deep accessor for getting elements in an internal array data structure at the given
     * position. It returns new objects (GenericArray/GenericMap/GenericMap) for nested
     * array/map/row types.
     *
     * <p>NOTE: Currently, it is only used for deep copying {@link ColumnarRow} for Arrow which
     * avoid the arrow buffer is released before accessing elements. It doesn't deep copy STRING and
     * BYTES types, because {@link ColumnarRow} already deep copies the bytes, see {@link
     * VectorizedColumnBatch#getString(int, int)}. This can be removed once we supports object reuse
     * for Arrow {@link ColumnarRow}, see {@code CompletedFetch#toScanRecord(LogRecord)}.
     */
    // 为特定的字段创建**深拷贝（Deep）**访问器。
    // 主要用于处理复杂嵌套类型（Array, Map, Row）
    // 在处理列式存储（Arrow 格式）时，为了防止底层的内存 Buffer 被释放导致无法访问，需要将嵌套结构的数据真正地拷贝出来，转换为 GenericArray 或 GenericMap。
    static FieldGetter createDeepFieldGetter(DataType fieldType, int fieldPos) {
        final FieldGetter fieldGetter;
        switch (fieldType.getTypeRoot()) {
            case ARRAY:
                DataType elementType = ((ArrayType) fieldType).getElementType();
                InternalArray.ElementGetter nestedGetter = createDeepElementGetter(elementType);
                fieldGetter =
                        row -> {
                            InternalArray array = row.getArray(fieldPos);
                            Object[] objs = new Object[array.size()];
                            for (int i = 0; i < array.size(); i++) {
                                objs[i] = nestedGetter.getElementOrNull(array, i);
                            }
                            return new GenericArray(objs);
                        };
                break;
            case MAP:
                MapType mapType = (MapType) fieldType;
                InternalArray.ElementGetter keyGetter =
                        createDeepElementGetter(mapType.getKeyType());
                InternalArray.ElementGetter valueGetter =
                        createDeepElementGetter(mapType.getValueType());
                fieldGetter =
                        row -> {
                            InternalMap map = row.getMap(fieldPos);
                            Map<Object, Object> javaMap = new HashMap<>();
                            for (int i = 0; i < map.size(); i++) {
                                Object key = keyGetter.getElementOrNull(map.keyArray(), i);
                                Object value = valueGetter.getElementOrNull(map.valueArray(), i);
                                javaMap.put(key, value);
                            }
                            return new GenericMap(javaMap);
                        };
                break;
            case ROW:
                RowType rowType = (RowType) fieldType;
                int numFields = rowType.getFieldCount();
                FieldGetter[] nestedFieldGetters = new FieldGetter[numFields];
                for (int i = 0; i < numFields; i++) {
                    nestedFieldGetters[i] = createDeepFieldGetter(rowType.getTypeAt(i), i);
                }
                fieldGetter =
                        row -> {
                            InternalRow nestedRow = row.getRow(fieldPos, numFields);
                            GenericRow genericRow = new GenericRow(numFields);
                            for (int i = 0; i < numFields; i++) {
                                genericRow.setField(
                                        i, nestedFieldGetters[i].getFieldOrNull(nestedRow));
                            }
                            return genericRow;
                        };
                break;
            default:
                // for primitive types, use the normal field getter
                fieldGetter = createFieldGetter(fieldType, fieldPos);
                break;
        }

        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /** Accessor for getting the field of a row during runtime. */
    interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(InternalRow row);
    }
}
