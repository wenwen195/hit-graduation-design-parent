package cn.hit.gd.api.processor;

import cn.hit.gd.api.MyStream;
import cn.hit.gd.api.processor.functions.ReassignTimestampOperator;
import cn.hit.gd.api.processor.functions.RetractStreamFilterFunction;
import cn.hit.gd.api.processor.functions.RetractStreamMapFunction;
import cn.hit.gd.api.util.FieldsUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: cn.hit.gd.api.processor
 * @date:2019/3/28
 **/
public class MultiInputStreamProcessor {
    private final String sql;
    private final List<String> inputTableNames;

    public MultiInputStreamProcessor(String sql, List<String> inputTableNames) {
        this.sql = sql;
        this.inputTableNames = inputTableNames;
    }

    public MyStream process(MyStream... myStreams) {
        if (myStreams.length != inputTableNames.size()) {
            throw new RuntimeException();
        }
        StreamExecutionEnvironment env = myStreams[0].getEnv();
        StreamTableEnvironment tableEnv = myStreams[0].getTableEnv();

        int i = 0;
        for (MyStream myStream : myStreams) {
            if (myStream.getEnv().equals(env)) {
                DataStream<Row> dataStream = myStream.getDataStream();
                if (!tableEnv.isRegistered(inputTableNames.get(i))) {
                    RowTypeInfo rowTypeInfo = (RowTypeInfo) dataStream.getType();
                    tableEnv.registerDataStream(inputTableNames.get(i), dataStream, FieldsUtil.getFieldsName(rowTypeInfo));
                }
            }
            i++;
        }
        Table table;
        try {
            table = tableEnv.sqlQuery(sql);
        } catch (Exception e) {
            throw new RuntimeException();
        }
        RowTypeInfo outRowTypeInfo = new RowTypeInfo(
                FieldsUtil.changeTimeIndicatoeTypeInfo(table.getSchema().getFieldTypes()),
                table.getSchema().getColumnNames()
        );
        return new MyStream(
                tableEnv.toRetractStream(table, outRowTypeInfo)
                        .filter(new RetractStreamFilterFunction()).name("Filter True Tuple")
                        .map(new RetractStreamMapFunction()).name("Get tuple elements")
                        .transform(ReassignTimestampOperator.class.getSimpleName(), outRowTypeInfo, new ReassignTimestampOperator())
                , FieldsUtil.rowTypeInfo2StreamFields(outRowTypeInfo)
                , env, tableEnv
        );
    }
}
