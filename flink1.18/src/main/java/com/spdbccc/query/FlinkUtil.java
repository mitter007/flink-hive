package com.spdbccc.query;

import com.spdbccc.bean.DataRow;
import com.spdbccc.bucket.FileSinkBucketAssigner;
import com.spdbccc.common.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.IOException;
import java.time.Duration;

/**
 * ClassName: FlinkSourceUtil
 * Package: com.spdbccc.hdfs.query
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/5 16:42
 * @Version 1.0
 */
public class FlinkUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                //.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                //在生产环境中，一般为了保证消费的精准一次性，需要手动维护偏移量，KafkaSource->KafkaSourceReader->存储偏移量变量
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 从最末尾位点开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                //注意：如果使用Flink提供的SimpleStringSchema对String类型的消息进行反序列化，如果消息为空，会报错
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null) {
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )

                .build();


        return kafkaSource;
    }


    public static FileSink<DataRow> getsink(String root_dir) {

        FileSink<DataRow> sink = FileSink
                .forRowFormat(new Path(root_dir),
                        new SimpleStringEncoder<DataRow>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMillis(15))
                                .withInactivityInterval(Duration.ofMillis(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build())
                .withBucketAssigner(new FileSinkBucketAssigner())

                .build();
        return sink;

    }

    public static PulsarSource<String> getPulsarSource(){
        PulsarSource<String> pulsarSource = PulsarSource.builder()
                .setServiceUrl(Constant.SERVICE_URL)
//                .setAdminUrl(adminUrl)
                .setStartCursor(StartCursor.earliest())
                .setTopics("test01")
                .setDeserializationSchema(new DeserPulsar())
                .setSubscriptionName("my-subscription")
                .build();
        return pulsarSource;

    }

}
