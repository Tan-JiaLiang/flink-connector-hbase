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
public abstract class WritableMetadata<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract T read(RowData row);

    /** Timestamp metadata for HBase. */
    public static class TimestampMetadata extends WritableMetadata<Long> {

        public static final String KEY = "timestamp";
        public static final DataType DATA_TYPE =
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable();

        private final int pos;

        public TimestampMetadata(int pos) {
            this.pos = pos;
        }

        @Override
        public Long read(RowData row) {
            if (row.isNullAt(pos)) {
                throw new IllegalArgumentException(
                        String.format("Writable metadata '%s' can not accept null value", KEY));
            }
            return row.getTimestamp(pos, 3).getMillisecond();
        }
    }
}
