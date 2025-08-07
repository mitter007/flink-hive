package com.spdbccc.query;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

/**
 * ClassName: DeserPulsar
 * Package: com.spdbccc.query
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/5 22:55
 * @Version 1.0
 */
public class DeserPulsar implements PulsarDeserializationSchema<String> {
    @Override
    public void deserialize(Message<byte[]> message, Collector<String> out) throws Exception {
        MessageId messageId = message.getMessageId();
        String topicName = message.getTopicName();
        long publishTime = message.getPublishTime();
        byte[] data = message.getData();
        JSONObject jsonObject = JSON.parseObject(new String(data));
        jsonObject.put("messageId",messageId);
        jsonObject.put("publishTime",publishTime);
        out.collect(jsonObject.toJSONString());



    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
