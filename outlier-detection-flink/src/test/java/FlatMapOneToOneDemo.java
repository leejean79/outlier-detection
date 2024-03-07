

    import org.apache.flink.api.common.functions.FlatMapFunction;
    import org.apache.flink.streaming.api.datastream.DataStream;
    import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
    import org.apache.flink.util.Collector;

    public class FlatMapOneToOneDemo {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> input = env.fromElements("apple", "banana", "cherry");

            DataStream<String> output = input.flatMap(new OneToManyFlatMap());

            output.print();

            env.execute("FlatMap One-to-Many Demo");
        }

        public static class OneToManyFlatMap implements FlatMapFunction<String, String> {
            @Override
            public void flatMap(String input, Collector<String> collector) {
                if (input.equals("apple")) {
                    collector.collect("apple");
                    collector.collect("red");
                    collector.collect("fruit");
                } else if (input.equals("banana")) {
                    collector.collect("banana");
                    collector.collect("yellow");
                    collector.collect("fruit");
                } else if (input.equals("cherry")) {
                    collector.collect("cherry");
                    collector.collect("red");
                    collector.collect("fruit");
                }
            }
        }
    }

