package com.boostarea.rocketmq.infrastructure.namesrv;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;

/**
 * Namesrv 启动类
 */
public class NamesrvStartup {

    public static void main(String[] args) throws Exception {
        // NamesrvConfig 配置
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        // NettyServerConfig 配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876); // 设置端口
        // 创建 NamesrvController 对象，并启动
        NamesrvController namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        namesrvController.initialize();
        namesrvController.start();
    }
}
