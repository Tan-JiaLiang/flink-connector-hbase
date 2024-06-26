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

package org.apache.flink.connector.hbase2;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.rules.TestName;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Plan tests for HBase connector, for example, testing projection push down. */
public class HBaseTablePlanTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    private TestInfo testInfo;

    @BeforeEach
    public void setup(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    // A workaround to get the test method name for Flink versions not completely migrated to JUnit5
    public TestName name() {
        return new TestName() {
            @Override
            public String getMethodName() {
                return testInfo.getTestMethod().get().getName();
            }
        };
    }

    @Test
    public void testMultipleRowKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE hTable ("
                                + " family1 ROW<col1 INT>,"
                                + " family2 ROW<col1 STRING, col2 BIGINT>,"
                                + " rowkey INT,"
                                + " rowkey2 STRING "
                                + ") WITH ("
                                + " 'connector' = 'hbase-2.2',"
                                + " 'table-name' = 'my_table',"
                                + " 'zookeeper.quorum' = 'localhost:2021'"
                                + ")");

        assertThatThrownBy(() -> util.verifyExecPlan("SELECT * FROM hTable"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Row key can't be set multiple times.");
    }

    @Test
    public void testNoneRowKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE hTable ("
                                + " family1 ROW<col1 INT>,"
                                + " family2 ROW<col1 STRING, col2 BIGINT>"
                                + ") WITH ("
                                + " 'connector' = 'hbase-2.2',"
                                + " 'table-name' = 'my_table',"
                                + " 'zookeeper.quorum' = 'localhost:2021'"
                                + ")");

        assertThatThrownBy(() -> util.verifyExecPlan("SELECT * FROM hTable"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "HBase table requires to define a row key field. "
                                + "A row key field is defined as an atomic type, "
                                + "column families and qualifiers are defined as ROW type.");
    }

    @Test
    public void testInvalidPrimaryKey() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE hTable ("
                                + " family1 ROW<col1 INT>,"
                                + " family2 ROW<col1 STRING, col2 BIGINT>,"
                                + " rowkey STRING, "
                                + " PRIMARY KEY (family1) NOT ENFORCED "
                                + ") WITH ("
                                + " 'connector' = 'hbase-2.2',"
                                + " 'table-name' = 'my_table',"
                                + " 'zookeeper.quorum' = 'localhost:2021'"
                                + ")");

        assertThatThrownBy(() -> util.verifyExecPlan("SELECT * FROM hTable"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Primary key of HBase table must be defined on the row key field. "
                                + "A row key field is defined as an atomic type, "
                                + "column families and qualifiers are defined as ROW type.");
    }

    @Test
    public void testUnsupportedDataType() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE hTable ("
                                + " family1 ROW<col1 INT>,"
                                + " family2 ROW<col1 STRING, col2 BIGINT>,"
                                + " col1 ARRAY<STRING>, "
                                + " rowkey STRING, "
                                + " PRIMARY KEY (rowkey) NOT ENFORCED "
                                + ") WITH ("
                                + " 'connector' = 'hbase-2.2',"
                                + " 'table-name' = 'my_table',"
                                + " 'zookeeper.quorum' = 'localhost:2021'"
                                + ")");

        assertThatThrownBy(() -> util.verifyExecPlan("SELECT * FROM hTable"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Unsupported field type 'ARRAY<STRING>' for HBase.");
    }

    @Test
    public void testProjectionPushDown() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE hTable ("
                                + " family1 ROW<col1 INT>,"
                                + " family2 ROW<col1 STRING, col2 BIGINT>,"
                                + " family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>,"
                                + " rowkey INT,"
                                + " PRIMARY KEY (rowkey) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'hbase-2.2',"
                                + " 'table-name' = 'my_table',"
                                + " 'zookeeper.quorum' = 'localhost:2021'"
                                + ")");
        util.verifyExecPlan("SELECT h.family3, h.family2.col2 FROM hTable AS h");
    }
}
