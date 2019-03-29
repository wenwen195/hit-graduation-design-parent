package cn.hit.gd.api.processor.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: cn.hit.gd.api.processor.functions
 * @date:2019/3/29
 **/
public class RetractStreamFilterFunction implements FilterFunction<Tuple2<Boolean, Row>> {
    @Override
    public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
        return booleanRowTuple2.f0;
    }
}
