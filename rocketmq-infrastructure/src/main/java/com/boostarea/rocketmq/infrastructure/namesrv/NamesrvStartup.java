package com.boostarea.rocketmq.infrastructure.namesrv;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;

/**
 * <p>文件名称:com.boostarea.rocketmq.infrastructure.namesrv.NamesrvStartup</p>
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
