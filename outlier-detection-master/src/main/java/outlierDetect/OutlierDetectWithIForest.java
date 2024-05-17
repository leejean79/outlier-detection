package outlierDetect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OutlierDetectWithIForest {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

    }
}
