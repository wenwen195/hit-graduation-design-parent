package cn.hit.gd.api.processor.functions;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * @author: create by ywrao
 * @version: v1.0
 * @description: cn.hit.gd.api.processor.functions
 * @date:2019/3/29
 **/
public class ReassignTimestampOperator extends AbstractStreamOperator<Row> implements OneInputStreamOperator<Row, Row> {

    Long eightHours = 8 * 60 * 60 * 1000L;

    public ReassignTimestampOperator() {
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<Row> streamRecord) throws Exception {
        boolean hasTimestamp = streamRecord.hasTimestamp();
        if (!hasTimestamp) {
            streamRecord.setTimestamp(((Timestamp) streamRecord.getValue().getField(streamRecord.getValue().getArity() - 1)).getTime() + eightHours);
        }
        output.collect(streamRecord);
    }
}
