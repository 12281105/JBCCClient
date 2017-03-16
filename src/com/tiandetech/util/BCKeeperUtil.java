package com.tiandetech.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j

/**
 * 打开连接
 */
public class BCKeeperUtil {

	public static String zookeeperConnectString;
	
	private static CuratorFramework client = null;

	
	public static CuratorFramework getClient()
	{
		
		if(client!=null)
		{
			return client;
		}
		else
		{
			open();
			return client;
		}
		
		
	}
	
	
	public static void open() {
		try {
			client = CuratorFrameworkFactory.builder()
					.connectString(zookeeperConnectString).sessionTimeoutMs(5000)
					.connectionTimeoutMs(3000).retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();

			client.start();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void close() {

		try {

			CloseableUtils.closeQuietly(client);

		} catch (Exception e) {
			e.printStackTrace();

		}
	}
	
	
	public static void initTDBC(String zkConnectString) {
		try 
		{
			zookeeperConnectString=zkConnectString;
			
			if (BCKeeperUtil.getClient().checkExists().forPath(BCKeeperSharies.getTdngZookeeperRoot()) == null) 
			{
				BCKeeperUtil.getClient().create().forPath(BCKeeperSharies.getTdngZookeeperRoot(), "".getBytes());// 是否存在根节点
				log.info("New Add node:"+BCKeeperSharies.getTdngZookeeperRoot());
			}
			
			if (BCKeeperUtil.getClient().checkExists().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/isfixedleader") == null) 
			{
				BCKeeperUtil.getClient().create().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/isfixedleader", "true".getBytes());// 是否固定主节点
				log.info("New Add node:"+BCKeeperSharies.getTdngZookeeperRoot()+"/isfixedleader");
			}
			
			if (BCKeeperUtil.getClient().checkExists().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/leaderselector") == null) 
			{
				BCKeeperUtil.getClient().create().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/leaderselector", "".getBytes());// 用于leader选举
				log.info("New Add node:"+BCKeeperSharies.getTdngZookeeperRoot()+"/leaderselector");
			}
			
			if (BCKeeperUtil.getClient().checkExists().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/online") == null) 
			{
				BCKeeperUtil.getClient().create().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/online", "".getBytes());// 节点是否在线
				log.info("New Add node:"+BCKeeperSharies.getTdngZookeeperRoot()+"/online");
			}
			
			
			if (BCKeeperUtil.getClient().checkExists().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/isleaderbyzookeeper") == null) 
			{
				BCKeeperUtil.getClient().create().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/isleaderbyzookeeper", "true".getBytes());// 是否由zookeeper选leader，还是由节点的height+round选主节点
				log.info("New Add node:"+BCKeeperSharies.getTdngZookeeperRoot()+"/isleaderbyzookeeper");
			}
			
			if (BCKeeperUtil.getClient().checkExists().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/pubkey") == null) //所有节点的公钥存储
			{
				BCKeeperUtil.getClient().create().forPath(BCKeeperSharies.getTdngZookeeperRoot()+"/pubkey", "".getBytes());
				log.info("New Add node:"+BCKeeperSharies.getTdngZookeeperRoot()+"/pubkey");
			}
			
			

		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}

	}
	
	
}