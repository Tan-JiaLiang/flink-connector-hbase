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

import org.apache.flink.connector.hbase.sink.WritableMetadata.TimeToLiveMetadata;
import org.apache.flink.connector.hbase.sink.WritableMetadata.TimestampMetadata;
import org.apache.flink.connector.hbase.util.HBaseSerde;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.hadoop.hbase.client.Mutation;

import java.util.List;

/**
 * An implementation of {@link HBaseMutationConverter} which converts {@link RowData} into {@link
 * Mutation}.
 */
public class RowDataToMutationConverter implements HBaseMutationConverter<RowData> {
    private static final long serialVersionUID = 1L;

    private final HBaseTableSchema schema;
    private final String nullStringLiteral;
    private final boolean ignoreNullValue;
    private final boolean ignoreDelete;
    private final TimestampMetadata timestampMetadata;
    private final TimeToLiveMetadata timeToLiveMetadata;
    private transient HBaseSerde serde;

    public RowDataToMutationConverter(
            HBaseTableSchema schema,
            DataType physicalDataType,
            List<String> metadataKeys,
            String nullStringLiteral,
            boolean ignoreNullValue,
            boolean ignoreDelete) {
        this.schema = schema;
        this.nullStringLiteral = nullStringLiteral;
        this.ignoreNullValue = ignoreNullValue;
        this.ignoreDelete = ignoreDelete;
        this.timestampMetadata = new TimestampMetadata(metadataKeys, physicalDataType);
        this.timeToLiveMetadata = new TimeToLiveMetadata(metadataKeys, physicalDataType);
    }

    @Override
    public void open() {
        this.serde = new HBaseSerde(schema, nullStringLiteral, ignoreNullValue, ignoreDelete);
    }

    @Override
    public Mutation convertToMutation(RowData record) {
        Long timestamp = timestampMetadata.read(record);
        Long timeToLive = timeToLiveMetadata.read(record);
        RowKind kind = record.getRowKind();
        if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
            return serde.createPutMutation(record, timestamp, timeToLive);
        } else {
            return serde.createDeleteMutation(record, timestamp);
        }
    }
}
