package com.realtime.yonghao.flink.connector.mongodb;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.internal.DebeziumOffset;
import io.debezium.connector.mongodb.MongoDbConnector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class MongoDBSQLSource {

    private static final String DATABASE_SERVER_NAME = "mongodb_oplog_source";

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder class of {@link MongoDBSQLSource}.
     */
    public static class Builder<T> {

        private String hosts;
        private String[] collectionList;
        private String username;
        private String password;
        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public Builder<T> hosts(String hosts) {
            this.hosts = hosts;
            return this;
        }

        public Builder<T> collectionList(String... collectionList) {
            this.collectionList = collectionList;
            return this;
        }

        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
            props.setProperty("connector.class", MongoDbConnector.class.getCanonicalName());

            props.setProperty("mongodb.hosts", checkNotNull(hosts));
            props.setProperty("mongodb.name", DATABASE_SERVER_NAME);
            props.setProperty("mongodb.user", checkNotNull(username));
            props.setProperty("mongodb.password", checkNotNull(password));

            if (collectionList != null) {
                props.setProperty("collection.include.list", String.join(",", collectionList));
            }

            DebeziumOffset specificOffset = new DebeziumOffset();
            return new DebeziumSourceFunction<>(
                    deserializer, props, specificOffset);
        }
    }
}
