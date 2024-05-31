package com.boostarea.rocketmq.feature.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * <p>文件名称:com.boostarea.rocketmq.feature.producer.NormalMessageProducer</p>
 * <p>文件描述: </p>
 * <p>版权所有: Copyright(C)2019-2022</p>
 * <p>公司: 趣店集团 </p>
 * <p>内容摘要: </p>
 * <p>其他说明: </p>
 *
 * @author <a> href="mailto:chenrong@qudian.com">chen</a>
 * @version 1.0
 * @since 2024/5/31
 */
public class NormalMessageProducer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("simple_producer_group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        for (int i = 0; i < 100; i++)
            try {
                {
                    Message msg = new Message("TopicTest",
                            "TagA",
                            "OrderID188",
                            "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        producer.shutdown();
    }

}
