package com.jm.flink.delta;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.source.DeltaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/25 11:17
 */
public class DeltaTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DeltaSource<RowData> deltaSource = DeltaSource.forContinuousRowData(
                        new Path("/Users/jinmu/Downloads/lakehouse/delta/jinmu.db/test1"),
                        new Configuration())
                .build();


        DataStreamSource<RowData> source = env.fromSource(deltaSource, WatermarkStrategy.noWatermarks(), "delta-source");

        source.print();

        List<RowType.RowField> fields =new ArrayList<>();
        fields.add(new RowType.RowField("name",new VarCharType()));
        fields.add(new RowType.RowField("age",new VarCharType()));
        fields.add(new RowType.RowField("insert_time",new TimestampType()));
        RowType rowType = new RowType(fields);
        DeltaSink<RowData> deltaSink = DeltaSink
                .forRowData(
                        new Path("/Users/jinmu/Downloads/lakehouse/delta/jinmu.db/test2"),
                        new Configuration(),
                        rowType
                        )
                .build();

        source.sinkTo(deltaSink);

        env.execute();
    }
}
