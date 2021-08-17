package com.realtime.yonghao.flink.connector.mongodb;

import com.alibaba.ververica.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

public class MongoDBTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "mongodb-cdc";

    private static final ConfigOption<String> HOSTS = ConfigOptions.key("hosts")
            .stringType()
            .noDefaultValue()
            .withDescription("IP address or hostname of the MongoDB database server.");

    private static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("Name of the MySQL database to use when connecting to the MongoDB database server.");

    private static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Password to use when connecting to the MongoDB database server.");

    private static final ConfigOption<String> COLLECTION = ConfigOptions.key("collection")
            .stringType()
            .noDefaultValue()
            .withDescription("Collection name of the MongoDB database server.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hosts = config.get(HOSTS);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String collectionName = config.get(COLLECTION);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new MongoDBTableSource(physicalSchema, hosts, username, password, collectionName);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(COLLECTION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }
}
