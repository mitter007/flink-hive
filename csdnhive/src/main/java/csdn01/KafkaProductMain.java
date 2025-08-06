package csdn01;
 
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
 
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
 
/**
 * @author liujiapeng.wb
 * @ClassName KafkaProductMain
 * @Description TODO
 * @date 2022/3/2 15:19
 * @Version 1.0
 */
public class KafkaProductMain {
    public static final Random random = new Random();
    public static void main(String[] args) throws Exception {
        // kafkaproduct-0.1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 
//        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);
        DataStreamSource<String> text = env.addSource(new MyJsonParalleSource()).setParallelism(1);
 
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        //new FlinkKafkaProducer("topn",new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
//        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("test_flink",new SimpleStringSchema(),properties);
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("test",new SimpleStringSchema(),properties);
/*
        //event-timestamp事件的发生时间
        producer.setWriteTimestampToKafka(true);
*/
        text.addSink(producer);
        env.execute();
    }
}