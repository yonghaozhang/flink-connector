package com.realtime.yonghao.flink.connector.mongodb;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.time.ZoneId;

public class MongoDBTableSource implements ScanTableSource {

    private final TableSchema physicalSchema;
    private final String hosts;
    private final String username;
    private final String password;
    private final String collectionName;

    public MongoDBTableSource(TableSchema physicalSchema, String hosts, String username, String password, String collectionName){
        this.physicalSchema = physicalSchema;
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.collectionName = collectionName;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(physicalSchema.toRowDataType());
        DebeziumDeserializationSchema<RowData> deserializer = new RowDataDebeziumDeserializeSchema(
                rowType, typeInfo, ((rowData, rowKind) -> {}), ZoneId.of("UTC"));
        MongoDBSQLSource.Builder<RowData> builder = MongoDBSQLSource.<RowData>builder()
                .hosts(hosts)
                .collectionList(collectionName)
                .username(username)
                .password(password)
                .deserializer(deserializer);
        DebeziumSourceFunction<RowData> sourceFunction = builder.build();
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new MongoDBTableSource(physicalSchema, hosts, username, password, collectionName);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB-CDC";
    }
}
