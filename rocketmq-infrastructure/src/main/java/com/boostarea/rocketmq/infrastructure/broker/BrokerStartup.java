package com.boostarea.rocketmq.infrastructure.broker;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * <p>文件名称:com.boostarea.rocketmq.infrastructure.broker.BrokerStartup</p>
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
public class BrokerStartup {

    public static void main(String[] args) throws Exception {
        // 设置版本号
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // NettyServerConfig 配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10911);
        // BrokerConfig 配置
        final BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName("broker-a");
        brokerConfig.setNamesrvAddr("127.0.0.1:9876");
        // MessageStoreConfig 配置
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setDeleteWhen("04");
        messageStoreConfig.setFileReservedTime(48);
        messageStoreConfig.setFlushDiskType(FlushDiskType.ASYNC_FLUSH);
        messageStoreConfig.setDuplicationEnable(false);

//        BrokerPathConfigHelper.setBrokerConfigPath("/Users/yunai/百度云同步盘/开发/Javascript/Story/incubator-rocketmq/conf/broker.conf");
        // 创建 BrokerController 对象，并启动
        BrokerController brokerController = new BrokerController(//
                brokerConfig, //
                nettyServerConfig, //
                new NettyClientConfig(), //
                messageStoreConfig);
        brokerController.initialize();
        brokerController.start();
    }
}
