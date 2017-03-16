/**
 * 
 */
package com.tiandetech.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Bob
 *
 */
@Slf4j
public class BCKeeperSharies 
{
	private static boolean isLeader=false;           //本节点是否是leader节点
	private static boolean isGenerateBlockDone=false; //leader节点是否完成建块操作
	private static HashMap<String,String> nodesHostNames = new HashMap<String,String>(); //以后不用IP ，全都用hostname
	public static String localHostName = "";
	public static String localIP= "";

	private static String tdngZookeeperRoot;//当前集群的配置管理在zookeeper中的根路径名称
	
	public static String getTdngZookeeperRoot() {
		return tdngZookeeperRoot;
	}
	public static void setTdngZookeeperRoot(String tdngZookeeperRoot) {
		BCKeeperSharies.tdngZookeeperRoot = tdngZookeeperRoot;
	}



	static {
		try {
			localHostName = InetAddress.getLocalHost().getHostName();
			localIP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public static boolean isLeader() {
		return isLeader;
	}
	public static void setLeader(boolean isLeader) {
		BCKeeperSharies.isLeader = isLeader;
	}
	public static boolean isGenerateBlockDone() {
		return isGenerateBlockDone;
	}
	public static void setGenerateBlockDone(boolean isGenerateBlockDone) {
		BCKeeperSharies.isGenerateBlockDone = isGenerateBlockDone;
	}
	
	/**
	 * 根据host name 查询是否可以参与建块
	 * @param hostName
	 * @return boolean
	 * @author xiaoming
	 */
	public synchronized static boolean isLeaderModeByHostName(String hostName){
		String node_local = nodesHostNames.get(hostName);
		if(node_local == null){
			return false;
		}
		JSONObject onLineHostJ = JSONObject.parseObject(node_local);
		if(onLineHostJ == null){
			return false;
		}
		return Boolean.parseBoolean(onLineHostJ.getString("isLeaderMode"));
	}
	
	/**
	 * 获取leader mode = true 的所有节点 names
	 * @return
	 */
	public synchronized static String[] getNodesHostNames() {
		/* 修改只获取能参与建块的ip  小明  2016-12-29 */
		List<String> addList = new ArrayList<String>();
		Set<String> keys = nodesHostNames.keySet();
		Iterator it = keys.iterator();
		while(it.hasNext()){
			String key = (String)it.next();
			String onLineStr = nodesHostNames.get(key);
			JSONObject onLineJ = JSONObject.parseObject(onLineStr);
			String isLeaderMode = onLineJ.getString("isLeaderMode");
			if("true".equals(isLeaderMode)){
				//能参与建块的ip
				addList.add(key);//保存host name
			}
		}
		String[] str = new String[addList.size()];
		addList.toArray(str);
		return str;
	}
	/**
	 * 获取除了自己以外在线节点数
	 * @return
	 */
	public synchronized static String[] getNodesHostNamesUnHost(String localHostName) {
		String[] onLines = getNodesHostNames();
		List<String> addList = new ArrayList<String>();
		for(String node:onLines){
			if(!node.equals(localHostName)){
				addList.add(node);
			}
		}
		String[] str = new String[addList.size()];
		addList.toArray(str);
		return str;
	}
	
	public synchronized static void setNodesHostNames(String  key,String value) 
	{
		BCKeeperSharies.nodesHostNames.put(key, value);//key=node_113   value={"ip":"192.168.0.113","isLeaderMode":"true"}
	}
	
	
	public synchronized static void removeNodesHostNames(String  key) 
	{
		BCKeeperSharies.nodesHostNames.remove(key);
	}
	
	/**
	 * 除了自己参与建块的在线节点数    实时
	 * @return
	 */
    public static int onLineLeaderModeNodesUnLocalHost(){
    	int nodes = 0;
    	try {
    		//刷新onlines
			List<String> onLines = BCKeeperUtil.getClient().getChildren().forPath(tdngZookeeperRoot+"/online");
			for(String on_node:onLines){
				String nodeJ = new String(BCKeeperUtil.getClient().getData().forPath(tdngZookeeperRoot+"/online/"+on_node), "utf-8");
				nodesHostNames.put(on_node, nodeJ);
			}
			nodes = getNodesHostNamesUnHost(BCKeeperSharies.localHostName).length;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
    	return nodes;
    }
    
}
