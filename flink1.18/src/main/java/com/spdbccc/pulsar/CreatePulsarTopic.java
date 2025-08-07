package com.spdbccc.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

public class CreatePulsarTopic {
    public static void main(String[] args) {
        String adminUrl = "http://hadoop102:8080";  // Pulsar broker 的 HTTP 服务地址
        String topicName = "persistent://public/default/event";
        int partitions = 4;  // 分区数

        try {
            // 创建 Admin 客户端
            PulsarAdmin admin = PulsarAdmin.builder()
                    .serviceHttpUrl(adminUrl)
                    .build();

            // 创建分区 Topic
            admin.topics().createPartitionedTopic(topicName, partitions);
            System.out.printf("✅ 分区 Topic 创建成功: %s, 分区数: %d\n", topicName, partitions);

            admin.close();
        } catch (PulsarAdminException e) {
            System.err.println("❌ 创建失败: " + e.getMessage());
            e.printStackTrace();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
