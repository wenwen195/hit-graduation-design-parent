package cn.hit.gd.api.connecter;

import cn.hit.gd.api.MyStream;
import cn.hit.gd.api.StreamFields;
import cn.hit.gd.api.util.FieldsUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: cn.hit.gd.api.connecter
 * @date:2019/3/29
 **/
public class CsvSource {
    private CsvTableSource csvTableSource;
    private final StreamFields streamFields;

    public CsvSource(String path, StreamFields streamFields) {
        this.streamFields = streamFields;
        RowTypeInfo rowTypeInfo = FieldsUtil.streamFields2RowTypeInfo(streamFields);
        this.csvTableSource = new CsvTableSource(path, rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
    }

    public MyStream getStream(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        return new MyStream(csvTableSource.getDataStream(env), streamFields, env, tableEnv);
    }


}
