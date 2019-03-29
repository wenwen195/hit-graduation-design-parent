package cn.hit.gd.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: cn.hit.gd.api
 * @date:2019/3/28
 **/
public class MyStream {
    private DataStream<Row> dataStream;
    private StreamFields streamFields;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;

    public MyStream(DataStream<Row> dataStream, StreamFields streamFields, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        this.dataStream = dataStream;
        this.streamFields = streamFields;
        this.env = env;
        this.tableEnv = tableEnv;
    }

    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public void setEnv(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public StreamTableEnvironment getTableEnv() {
        return tableEnv;
    }

    public void setTableEnv(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
    }

    public DataStream<Row> getDataStream() {
        return dataStream;
    }

    public void setDataStream(DataStream<Row> dataStream) {
        this.dataStream = dataStream;
    }

    public StreamFields getStreamFields() {
        return streamFields;
    }

    public void setStreamFields(StreamFields streamFields) {
        this.streamFields = streamFields;
    }
}
