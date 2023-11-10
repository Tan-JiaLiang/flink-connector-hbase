/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

/** Writable metadata for HBase. */
public enum WritableMetadata {
    TIMESTAMP(
            "timestamp",
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
            new MetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Object read(RowData row, int pos) {
                    if (row.isNullAt(pos)) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Writable metadata %s can not accept null value",
                                        TIMESTAMP.key));
                    }
                    return row.getTimestamp(pos, 3).getMillisecond();
                }
            });

    final String key;

    final DataType dataType;

    final MetadataConverter converter;

    WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }
}
