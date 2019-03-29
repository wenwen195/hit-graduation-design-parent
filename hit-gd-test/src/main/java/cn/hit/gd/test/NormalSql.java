package cn.hit.gd.test;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: PACKAGE_NAME
 * @date:2019/1/28
 **/
public class NormalSql {
    StreamExecutionEnvironment env;
    StreamTableEnvironment tableEnv;

    @Before
    public void before(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = TableEnvironment.getTableEnvironment(env);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    @Test
    public void testEasySql() throws Exception {
        List<Row> leftData=new ArrayList<Row>();
        leftData.add(Row.of("111",new BigDecimal("60"),2000L));

        TypeInformation<?>[] leftTypes={
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.BIG_DEC_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };

        String[] leftNames={"security_code","close_price","quotation_time"};
        RowTypeInfo leftTypeInfo=new RowTypeInfo(leftTypes,leftNames);

        DataStream<Row> leftDataStream=env.fromCollection(leftData).returns(leftTypeInfo).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return (Long)row.getField(row.getArity()-1);
            }
        });

        leftDataStream.print();

        //TODO:抄一下multiProcess  StreamSchema
        Table leftIn=tableEnv.fromDataStream(leftDataStream,"security_code, close_price, quotation_time.rowtime");
        tableEnv.registerTable("myLeftTable",leftIn);

        String sql="SELECT * from myLeftTable";

        Table result=tableEnv.sqlQuery(sql);

        DataStream<Row> resultDs=tableEnv.toAppendStream(result,Row.class);

        resultDs.print();

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    @Test
    public void testTemporalSql() throws Exception {
        List<Row> leftData=new ArrayList<Row>();
        leftData.add(Row.of("111",new BigDecimal("60"),2000L));

        TypeInformation<?>[] leftTypes={
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.BIG_DEC_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };

        String[] leftNames={"security_code","close_price","quotation_time"};
        RowTypeInfo leftTypeInfo=new RowTypeInfo(leftTypes,leftNames);

        DataStream<Row> leftDataStream=env.fromCollection(leftData).returns(leftTypeInfo).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return (Long)row.getField(row.getArity()-1);
            }
        });

//        leftDataStream.print();

        //TODO:抄一下multiProcess  StreamSchema
        Table leftIn=tableEnv.fromDataStream(leftDataStream,"security_code, close_price, quotation_time.rowtime");
        tableEnv.registerTable("myLeftTable",leftIn);

        List<Row> rightData=new ArrayList<Row>();
        rightData.add(Row.of("111",new BigDecimal("6000"),2000L));

        TypeInformation<?>[] rightTypes={
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.BIG_DEC_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };

        String[] rightNames={"security_code","trade_turnover","quotation_time"};
        RowTypeInfo rightTypeInfo=new RowTypeInfo(rightTypes,rightNames);

        DataStream<Row> rightDataStream=env.fromCollection(rightData).returns(rightTypeInfo).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return (Long)row.getField(row.getArity()-1);
            }
        });

//        rightDataStream.print();

        //TODO:抄一下multiProcess  StreamSchema
        Table rightIn=tableEnv.fromDataStream(rightDataStream,"security_code, trade_turnover, quotation_time.rowtime");
        tableEnv.registerTable("myRightTable",rightIn);

        TemporalTableFunction temporalTableFunction=rightIn.createTemporalTableFunction("quotation_time","security_code");
        tableEnv.registerFunction("Turnover",temporalTableFunction);

        String sql="SELECT  l.security_code,l.close_price,r.trade_turnover,l.quotation_time\n" +
                "from myLeftTable as l\n" +
                "   join LATERAL TABLE (Turnover(l.quotation_time)) as r\n" +
                "   on l.security_code=r.security_code";//也可加时间字段 <, >, = 限制，默认等于

        System.out.println(sql);
        Table result=tableEnv.sqlQuery(sql);

        DataStream<Row> resultDs=tableEnv.toAppendStream(result,Row.class);

        resultDs.print();

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    @Test
    public void testExternalTableSql() throws Exception{
        List<Row> leftData=new ArrayList<Row>();
        leftData.add(Row.of("111",new BigDecimal("60"),2000L));

        TypeInformation<?>[] leftTypes={
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.BIG_DEC_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };

        String[] leftNames={"security_code","close_price","quotation_time"};
        RowTypeInfo leftTypeInfo=new RowTypeInfo(leftTypes,leftNames);

        DataStream<Row> leftDataStream=env.fromCollection(leftData).returns(leftTypeInfo).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                return (Long)row.getField(row.getArity()-1);
            }
        });

//        leftDataStream.print();

        //TODO:抄一下multiProcess  StreamSchema
        Table leftIn=tableEnv.fromDataStream(leftDataStream,"security_code, close_price, quotation_time.rowtime");
        tableEnv.registerTable("myLeftTable",leftIn);

//        DataSet externalSet=

    }
}
