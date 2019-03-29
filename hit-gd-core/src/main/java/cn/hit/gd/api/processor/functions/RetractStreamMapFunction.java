package cn.hit.gd.api.processor.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: cn.hit.gd.api.processor.functions
 * @date:2019/3/29
 **/
public class RetractStreamMapFunction implements MapFunction<Tuple2<Boolean, Row>, Row> {
    @Override
    public Row map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
        return booleanRowTuple2.f1;
    }
}
