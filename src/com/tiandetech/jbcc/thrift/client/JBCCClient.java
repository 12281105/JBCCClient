package com.tiandetech.jbcc.thrift.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.asm.Type;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCException;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCResult;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService.AsyncClient;
import com.tiandetech.util.BCKeeperSharies;
import com.tiandetech.util.BCKeeperUtil;





public class JBCCClient {
	
	private static final JBCCClient instance = new JBCCClient(); //instance 单例模式
	
	private final int DEFAULTSYNCPORT = 8082;
	private final int DEFAULTASYNCPORT = 8083;

	//ip地址和ConnectionStruct的对应关系 
	private HashMap<String, ConnectionStruct> syncConnectionHashMap = new HashMap<String, ConnectionStruct>();
	
	//ip地址和异步连接信息AsyncConnection
	private HashMap<String, AsyncConnection> asyncConnectionHashMap = new HashMap<String, AsyncConnection>();
	
	private String zookeeperrootpath = null;
	private String zookeeperhosts = null;

	private int DEFAULTMAXCLIENT = 8;
	private int DEFAULTMAXCLIENTPERTHREAD = 8;
	
	private boolean TESTMODE = false;
	private String testingIP = "106.39.31.62";

	
	private JBCCClient() {
	}
	
	
	/**瀹㈡埛绔负鍗曚緥锛岀储瑕佸疄渚� // Obtain an instance of JBCC Client, which is a singleton type. 
	 * 
	 * @return  JBCCClient瀹炰緥 // JBCCClient instance
	 */
	public static JBCCClient getInstance(){
        return instance;
    }
	
	/**鍒濆鍖栧鎴风锛岄摼鎺ookeeper鏌ヨ鑺傜偣鍦板潃鍜岀姸鎬� // Initialize the client, and connect to zookeeper servers
	 * 
	 * @param zookeeperhosts  zookeeper鏈嶅姟鍣↖P鍦板潃锛岀敤閫楀彿闂撮殧 // zookeeper server addresses, separated by comma
	 * @param zookeeperrootpath zookeeper娉ㄥ唽鐨剅ootpath // zookeeper rootpath
	 */
	public void initializeClient(String zookeeperhosts, String zookeeperrootpath)
	{
		this.zookeeperrootpath = zookeeperrootpath;
		this.zookeeperhosts = zookeeperhosts;
		BCKeeperUtil.zookeeperConnectString = zookeeperhosts;
		startZooKeeperWatch(zookeeperrootpath); //参数为根目录路径，zookeeper watch，监听节点变化情况，节点活动事件注册处理函数
		System.out.println("zookeeper service watch started");
		
	}
	
	private void startZooKeeperWatch(String zkrootpath) {
		 final PathChildrenCache childrenCache4HeartBeat = new PathChildrenCache(BCKeeperUtil.getClient(), zkrootpath+"/online", true);
		
		 try {
			childrenCache4HeartBeat.start(StartMode.POST_INITIALIZED_EVENT);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	        childrenCache4HeartBeat.getListenable().addListener(
	            new PathChildrenCacheListener() {
	                @Override
	                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
	                        throws Exception {
	                        switch (event.getType()) {
	                        case CHILD_ADDED:
	                            
	                        	updateActiveConnections();
	                        	//System.out.println("New Node Detected");
	                            
	                            break;
	                        case CHILD_REMOVED:
	                        	
	                        	updateActiveConnections();
	                        	//System.out.println("Node Removed");
	                        	
	                        	break;
	                        case CHILD_UPDATED:
	                        	
	                        	updateActiveConnections(); //更新活动的连接
	                        	//System.out.println("Nodes Updated");
	                        	
	                        	break;
	                        case CONNECTION_RECONNECTED:
	                        	//UpdateActiveConnections();
	                        	break;
	                        default:
	                            break;
	                    }
	                }
	            }
	        );
		 
	}
	
	//节点事件处理
	private void updateActiveConnections()
	{
		//List<String> addresses = new ArrayList<String>();
		List<String> addresses = new ArrayList<String>();
		
		//测试模式和运行模式
		if (this.TESTMODE)
		{
			addresses.add(this.testingIP);
		}
		else
		{
			addresses = getNodeAddress(zookeeperrootpath);
		}
		
		if (!syncConnectionHashMap.isEmpty())
		{
			for (String entry : addresses) 
			{
				if(!syncConnectionHashMap.containsKey(entry))
				{
					TTransport transport;
					transport = new TSocket(entry, this.DEFAULTSYNCPORT);
					try {
						transport.open();
					} catch (TTransportException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					TProtocol protocol = new TBinaryProtocol(transport);
					JBCCService.Client myJBCCClient=new JBCCService.Client(protocol);
					ConnectionStruct _connectionStruct = new ConnectionStruct(myJBCCClient, transport, protocol);
					syncConnectionHashMap.put(entry, _connectionStruct);
				}
				
			}
			
			List<String> removednodes = new ArrayList<String>();
			
			for (Map.Entry<String,  ConnectionStruct> entry : syncConnectionHashMap.entrySet()) 
			{
				
				if(!addresses.contains(entry.getKey()))
				{
					entry.getValue().get_transport().close();
					removednodes.add(entry.getKey());
				}
				
			}
			
			for (String entry : removednodes)
			{
				syncConnectionHashMap.remove(entry);
			}

		}
		
		
		
		if (!asyncConnectionHashMap.isEmpty())
		{
			for (String entry : addresses) 
			{
				if(!asyncConnectionHashMap.containsKey(entry))
				{
					
					try
					{
						AsyncConnection t = new AsyncConnection(this.DEFAULTMAXCLIENT, this.DEFAULTMAXCLIENTPERTHREAD, entry, this.DEFAULTASYNCPORT);
						asyncConnectionHashMap.put(entry, t);
					}catch (Exception e)
					{}
					
				}
				
			}
			
			List<String> removednodes = new ArrayList<String>();
			
			for (Map.Entry<String,  AsyncConnection> entry : asyncConnectionHashMap.entrySet()) 
			{
				
				if(!addresses.contains(entry.getKey()))
				{
					entry.getValue().CloseConnections();
					removednodes.add(entry.getKey());
				}
				
			}
			
			for (String entry : removednodes)
			{
				asyncConnectionHashMap.remove(entry);
			}
		}
		
		
		
		return;
	}


	//from zookeeper
	private List<String> getNodeAddress(String rootpath)
	{
		List<String> childs = null;
		
		try {
			childs = BCKeeperUtil.getClient().getChildren().forPath(rootpath+"/online");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
		List<String> IPAddresses = new ArrayList<String>();
		
		if (childs != null)
		{
			for(String child : childs){
		    	byte[] data = null;
				try {
					data = BCKeeperUtil.getClient().getData().forPath(rootpath+"/online/"+child);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	JSONObject json = null;
		    	if (data != null)
		    	{
					try {
						json = JSON.parseObject(new String(data,"utf-8"));
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		    	}
		    	if (json != null)
		    	{
		    		String ip = json.getString("ip");
		    		if(ip!=null && !"".equals(ip.trim())){
		    			IPAddresses.add(ip);
		    		}
		    	}
		    }
		}
		
		return IPAddresses;	
		
	}
	

	//Change to service discovery
	//建立连接，并保存连接信息到syncConnectionHashMap
	private JBCCResult connectToService(String serviceName) throws TException
	{
			JBCCResult _jbccresult = new JBCCResult();
			
			List<String> addresses = new ArrayList<String>();
			
			if (this.TESTMODE)
			{
				addresses.add(this.testingIP);
			}
			else
			{
				addresses = getNodeAddress(zookeeperrootpath);
			}
			
			
			if (addresses == null || addresses.isEmpty())
			{
				_jbccresult.setStatus(0);
				_jbccresult.setMessage("No addresses from service discovery.");
				return _jbccresult;
			}
			
			try{
				for (String address : addresses)
				{
					TTransport transport;
					transport = new TSocket(address, this.DEFAULTSYNCPORT);
					transport.open();
					TProtocol protocol = new TBinaryProtocol(transport);
					JBCCService.Client myJBCCClient=new JBCCService.Client(protocol);
					ConnectionStruct _connectionStruct = new ConnectionStruct(myJBCCClient, transport, protocol);
					syncConnectionHashMap.put(address, _connectionStruct);
				}
			}
			catch(TException e)
			{
				_jbccresult.setStatus(0);
				_jbccresult.setMessage("Thrift exception.");
				return _jbccresult;
			}
			
			
			
			_jbccresult.setStatus(1);
			return _jbccresult;
	}
	
	//Change to service discovery
	private JBCCResult connectToServiceSecure(String serviceName, String truststore, String password) throws TException
		{
				int PORT = 7911;
				JBCCResult _jbccresult = new JBCCResult();
				
				List<String> addresses = new ArrayList<String>();
				
				if (this.TESTMODE)
				{
					addresses.add(this.testingIP);
				}
				else
				{
					addresses = getNodeAddress(zookeeperrootpath);
				}
				
				if (addresses == null || addresses.isEmpty())
				{
					_jbccresult.setStatus(0);
					_jbccresult.setMessage("No addresses from discovery.");
					return _jbccresult;
				}
				
				try{
					for (String address : addresses)
					{
						TTransport transport;
						
						TSSLTransportFactory.TSSLTransportParameters params =
			                    new TSSLTransportFactory.TSSLTransportParameters();
			            params.setTrustStore(truststore, password);
			 
			            transport = TSSLTransportFactory.getClientSocket(address, PORT, 10000, params);
			            TProtocol protocol = new TBinaryProtocol(transport);
			            JBCCService.Client client = new JBCCService.Client(protocol);
						ConnectionStruct _connectionStruct = new ConnectionStruct(client, transport, protocol);
						syncConnectionHashMap.put(address, _connectionStruct);
					}
				}
				catch(TException e)
				{
					_jbccresult.setStatus(0);
					_jbccresult.setMessage("Thrift exception.");
					return _jbccresult;
				}
				
				_jbccresult.setStatus(1);
				return _jbccresult;
		}
	
	
	/**鎵撳紑涓庤妭鐐圭兢鐨勫悓姝ヨ繛鎺�
	 * 
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯� 
	 * @throws TException Thrift寮傚父
	 */
	public JBCCResult openSyncConnection() throws TException
	{
		return connectToService("servicename");
	}
	
	/**鍏抽棴涓庤妭鐐圭兢鐨勫悓姝ヨ繛鎺�
	 * 
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯� 
	 */
	
	public JBCCResult closeSyncConnection()
	{
		JBCCResult _jbccresult = new JBCCResult();
		
		for (Map.Entry<String,  ConnectionStruct> entry : syncConnectionHashMap.entrySet()) 
		{
			entry.getValue().get_transport().close();
		}
		
		syncConnectionHashMap.clear();
		
		_jbccresult.setStatus(1);
		return _jbccresult;
	}
	

	private JBCCResult openSecureConnection(String servicename, String truststore, String password) throws TException
	{
		return connectToServiceSecure(servicename, truststore, password);
	}
	
	
	/**鎵撳紑涓庤妭鐐圭兢鐨勫紓姝ヨ繛鎺� // Open synchronous connection with the nodes.
	 * 
	 * @param max_client 鏈�澶歝lient瀹炰緥鏁板�� // max client instance number
	 * @param max_client_per_thread 姣忔潯绾夸笂鏈�澶歝lient瀹炰緥鏁板�� // max client instance number per thread
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯� // JBCCResult.status 0 is normal, 1 is exception
	 * @throws TException Thrift寮傚父 // Thrift exception
	 */
	public JBCCResult openAsyncConnection(int max_client, int max_client_per_thread) throws TException
	{
		//int PORT = 8989;//port
		
		JBCCResult _jbccresult = new JBCCResult();
		
		List<String> addresses = new ArrayList<String>();
		
		if (this.TESTMODE)
		{
			addresses.add(this.testingIP);
		}
		else
		{
			addresses = getNodeAddress(zookeeperrootpath);
		}
				
		if (addresses == null || addresses.isEmpty())
		{
			_jbccresult.setStatus(0);
			_jbccresult.setMessage("No addresses from discovery.");
			return _jbccresult;
		}
		
		try{
			for (String address : addresses)
			{

				AsyncConnection t = new AsyncConnection(max_client, max_client_per_thread, address, this.DEFAULTASYNCPORT);
				asyncConnectionHashMap.put(address, t);
			}
		}
		catch(IOException e)
		{
			_jbccresult.setStatus(0);
			_jbccresult.setMessage("Thrift IO exception.");
			return _jbccresult;
		}
		
		_jbccresult.setStatus(1);
		return _jbccresult;
	}
	
	/**鐢ㄩ粯璁よ缃墦寮�涓庤妭鐐圭兢鐨勫紓姝ヨ繛鎺�  // Open asynchronous connection with default parameters.
	 * 
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯�  // JBCCResult.status 0 is normal, 1 is exception
	 * @throws TException Thrift寮傚父  // Thrift exception
	 */
	
	public JBCCResult openAsyncConnection() throws TException
	{
		int PORT = 8989;//port
		
		JBCCResult _jbccresult = new JBCCResult();
		
		List<String> addresses = new ArrayList<String>();
		
		if (this.TESTMODE)
		{
			addresses.add(this.testingIP);
		}
		else
		{
			addresses = getNodeAddress(zookeeperrootpath);
		}
		
		
		if (addresses == null || addresses.isEmpty())
		{
			_jbccresult.setStatus(0);
			_jbccresult.setMessage("No addresses from discovery.");
			return _jbccresult;
		}
		
		try{
			for (String address : addresses)
			{

				AsyncConnection t = new AsyncConnection(this.DEFAULTMAXCLIENT, this.DEFAULTMAXCLIENTPERTHREAD, address, PORT);
				asyncConnectionHashMap.put(address, t);
			}
		}
		catch(IOException e)
		{
			_jbccresult.setStatus(0);
			_jbccresult.setMessage("Thrift IO exception.");
			return _jbccresult;
		}
		
		_jbccresult.setStatus(1);
		return _jbccresult;
	}
	
	/** 鍏抽棴鎵�鏈夊紓姝ヨ繛鎺� // Close all asynchronous connections. 
	 * 
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯� // JBCCResult.status 0 is normal, 1 is exception
	 */
	
	public JBCCResult closeAsyncConnection()
	{
		JBCCResult _jbccresult = new JBCCResult();
		
		for (Map.Entry<String,  AsyncConnection> entry : asyncConnectionHashMap.entrySet()) 
		{
			entry.getValue().CloseConnections();
		}
		
		asyncConnectionHashMap.clear();
		
		_jbccresult.setStatus(1);
		return _jbccresult;
	}
	
	/** 寮傛鎻掑叆浜ゆ槗 NOT IMPLEMENTED
	 * 
	 * @param transact TransactionBase绫� 
	 * @param results 鏀堕泦寮傛缁撴灉鐨凙rrayList
	 */
	
	public void asyncInsertToBC(Map<String, Object> transact, ArrayList<JBCCResult> results)
	{
		JBCCResult returnresult = new JBCCResult();
		
		if (asyncConnectionHashMap.isEmpty())
		{
			returnresult.setStatus(0);
			returnresult.setMessage("No open async connections.");
			results.add(returnresult);
			return;
		}
		
		String jsonString = JSON.toJSONString(transact);
		
		if (jsonString == null)
		{
			returnresult.setStatus(0);
			returnresult.setMessage("Serialization failed.");
			results.add(returnresult);
			return;
		}

		for (Map.Entry<String,  AsyncConnection> entry : asyncConnectionHashMap.entrySet()) 
		{
			if (entry.getValue() == null)
			{
				returnresult.setStatus(0);
				returnresult.setMessage("Could not retrieve Thrift client.");
				results.add(returnresult);
				return;
			}
	
			AsyncInsertRequest _insertRequest = new AsyncInsertRequest();
			_insertRequest.set_content(jsonString);
			//_insertRequest.set_TBCName(transact.blockchainType);
			_insertRequest.setResults(results);
			entry.getValue().req(_insertRequest);

		}
		
	    return;
	}
	
	/**寮傛鎵归噺鎻掑叆浜ゆ槗 //Asynchronous batch insert
	 * 
	 * @param transactlist 浜ゆ槗list // list of transactions
	 * @param BCName 鍖哄潡閾惧悕 // block chain name
	 * @param results 杩斿洖缁撴灉list // list of results
	 * @param delay 鏌ヨ钀藉潡寤惰繜 // delay for checking if the block was created
	 */
	
	public void asyncMultiInsertToBC(List<Map<String, Object>> transactlist, String BCName, ArrayList<JBCCResult> results, int delay)
	{
		JBCCResult returnresult = new JBCCResult();
		Map<String, String> transactmap = new HashMap<String, String>();
		
		if (asyncConnectionHashMap.isEmpty())
		{
			returnresult.setStatus(0);
			returnresult.setMessage("No open async connections.");
			results.add(returnresult);
			return;
		}
		
		for (Map<String, Object> transact : transactlist)
		{
			transact.put("blockchainType", BCName);
		    UUID uuid = UUID.randomUUID();
	        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        String timestamp = sdf.format(new Date());
	     
	        transact.put("edittime", timestamp);
	        transact.put("uuid", uuid.toString());
	        
	        String jsonString = JSON.toJSONString(this.maptoJSONObject(transact));
	        transactmap.put(uuid.toString(), jsonString);
		}
				
		for (Map.Entry<String,  AsyncConnection> entry : asyncConnectionHashMap.entrySet()) 
		{
			if (entry.getValue() == null)
			{
				returnresult.setStatus(0);
				returnresult.setMessage("Could not retrieve Thrift client.");
				results.add(returnresult);
				return;
			}
	
			AsyncMultiInsertRequest _multiinsertRequest = new AsyncMultiInsertRequest();
			_multiinsertRequest.set_delay(delay);
			_multiinsertRequest.set_myJBCCClient(this.getInstance());
			_multiinsertRequest.set_results(results);
			_multiinsertRequest.set_TBCName(BCName);
			_multiinsertRequest.set_transactmap(transactmap);
			
			entry.getValue().req(_multiinsertRequest);

		}
		
		
		
		
	}
	
	/** 鍚屾鎻掑叆浜ゆ槗 NOT IMPLEMENTED
	 * 
	 * @param transact TransactionBase绫� 
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯� 
	 * @throws TException Thrift寮傚父
	 */
	public List<JBCCResult> insertToBC (Map<String, Object> transact) throws TException
	{
		
			ArrayList<JBCCResult> returnresult = new ArrayList<JBCCResult>();
			JBCCResult _returnresult = new JBCCResult();
			
			if (syncConnectionHashMap.isEmpty())
			{
				_returnresult.setStatus(0);
				_returnresult.setMessage("No open connections.");
				returnresult.add(_returnresult);
				return returnresult;
			}
			
			String jsonString = JSON.toJSONString(transact);
			
			if (jsonString == null)
			{
				_returnresult.setStatus(0);
				_returnresult.setMessage("Serialization failed.");
				returnresult.add(_returnresult);
				return returnresult;
			}
			
			for (Map.Entry<String,  ConnectionStruct> entry : syncConnectionHashMap.entrySet()) 
			{
				
				JBCCService.Client myJBCCClient=entry.getValue().get_client();
				
				if (myJBCCClient == null)
				{
					_returnresult.setStatus(0);
					_returnresult.setMessage("Could not retrieve thrift client");
					returnresult.add(_returnresult);
					return returnresult;
				}
				
				try
		        {	
					JBCCResult returnresultback = myJBCCClient.insert((String)transact.get("blockchainType"), jsonString);   
					returnresult.add(returnresultback);
		        } catch (JBCCException e){
		        	_returnresult.setStatus(0);
					_returnresult.setMessage(e.getMessage());
					returnresult.add(_returnresult);
					return returnresult;
		        }

			}
			
		    return returnresult;
        
		
	} 
	/**蹇�熸彃鍏ヤ氦鏄� // Fast insert
	 * 
	 * @param transact 鐢ㄦ埛瀹氫箟鏁版嵁 瀛楁鍚嶏細鍊� // user defined data, field name  : field value  
	 * @param BCName 鍖哄潡閾惧悕 // blockchain name
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝2涓烘棤鐘舵�� // JBCCResult.status 0 is normal, 1 is exceptions
	 * @throws TException Thrift寮傚父 //thrift exception
	 */
	public List<JBCCResult> finsertToBC (Map<String, Object> transact, String BCName) throws TException
	{
		
			ArrayList<JBCCResult> returnresult = new ArrayList<JBCCResult>();
			JBCCResult _returnresult = new JBCCResult();
			
			if (syncConnectionHashMap.isEmpty())
			{
				_returnresult.setStatus(0);
				_returnresult.setMessage("No open connections.");
				returnresult.add(_returnresult);
				return returnresult;
			}
			
			transact.put("blockchainType", BCName);
		
			
		    UUID uuid = UUID.randomUUID();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String timestamp = sdf.format(new Date());
         
            transact.put("edittime", timestamp);
            transact.put("uuid", uuid.toString());
			
			String jsonString = JSON.toJSONString(this.maptoJSONObject(transact));
			
			if (jsonString == null)
			{
				_returnresult.setStatus(0);
				_returnresult.setMessage("Serialization failed.");
				returnresult.add(_returnresult);
				return returnresult;
			}
			
			for (Map.Entry<String,  ConnectionStruct> entry : syncConnectionHashMap.entrySet()) 
			{
				
				JBCCService.Client myJBCCClient=entry.getValue().get_client();
				
				if (myJBCCClient == null)
				{
					_returnresult.setStatus(0);
					_returnresult.setMessage("Could not retrieve thrift client");
					returnresult.add(_returnresult);
					return returnresult;
				}
				
				try
		        {	
					JBCCResult returnresultback = myJBCCClient.finsert(BCName, jsonString);   
					returnresult.add(returnresultback);
		        } catch (JBCCException e){
		        	_returnresult.setStatus(0);
					_returnresult.setMessage(e.getMessage());
					returnresult.add(_returnresult);
					return returnresult;
		        }

			}
			
		    return returnresult;
        
		
	} 
	
	/** NOT IMPLEMENTED
	 * 
	 * @param TBCName
	 * @param condition
	 * @return
	 * @throws TException
	 */

	private List<JBCCResult> selectFromBC (String TBCName, String condition) throws TException
	{
		
			ArrayList<JBCCResult> returnresult = new ArrayList<JBCCResult>();
			JBCCResult _returnresult = new JBCCResult();
			
			if (syncConnectionHashMap.isEmpty())
			{
				_returnresult.setStatus(0);
				_returnresult.setMessage("No open connections.");
				returnresult.add(_returnresult);
				return returnresult;
			}
			
			for (Map.Entry<String,  ConnectionStruct> entry : syncConnectionHashMap.entrySet()) 
			{

				
				JBCCService.Client myJBCCClient=entry.getValue().get_client();
				
				if (myJBCCClient == null)
				{
					_returnresult.setStatus(0);
					_returnresult.setMessage("Could not retrieve Thrift client.");
					returnresult.add(_returnresult);
					return returnresult;
				}
				
				try
		        {	
		            JBCCResult returnresultback = myJBCCClient.select(TBCName, condition);
		            returnresult.add(returnresultback);
		        } catch (JBCCException e){
		        	_returnresult.setStatus(0);
					_returnresult.setMessage(e.getMessage());
					returnresult.add(_returnresult);
					return returnresult;
		        }
				
			}
	
		    return returnresult;
		
	}
	
	/** NOT IMPLEMENTED
	 * 
	 * @param TBCName
	 * @param condition
	 * @param results
	 * @throws TException
	 */
	
	private void asyncSelectFromBC (String TBCName, String condition, ArrayList<JBCCResult> results) throws TException
	{
		
			JBCCResult returnresult = new JBCCResult();
			
			if (asyncConnectionHashMap.isEmpty())
			{
				returnresult.setStatus(0);
				returnresult.setMessage("No open connections.");
				results.add(returnresult);
				return;
			}
			
			for (Map.Entry<String,  AsyncConnection> entry : asyncConnectionHashMap.entrySet()) 
			{
				if (entry.getValue() == null)
				{
					returnresult.setStatus(0);
					returnresult.setMessage("Could not retrieve Thrift client.");
					results.add(returnresult);
					return;
				}
		
				AsyncSelectRequest _selectRequest = new AsyncSelectRequest();
				_selectRequest.set_condition(condition);
				_selectRequest.set_TBCName(TBCName);
				_selectRequest.setResults(results);
		        entry.getValue().req(_selectRequest);

			}
	
		    return;
		
	}
	
	
	/** 鍖哄潡閾句氦鏄撴暟鎹煡璇� // Transaction data lookup
	 * 
	 * @param BCName 鍖哄潡閾惧悕绉� // blockchain name
	 * @param condition SQL where 瀛楃涓� // SQL where conditional statement 
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝JBCCResult.message涓篞ueryResult绫荤殑JSON搴忓垪鍗庝覆
	 * @throws TException Thrift寮傚父 //Thrift exception
	 */
	
	public JBCCResult fastSelectFromTransaction (String BCName, String condition) throws TException
	{
		return fastSelectFromBC("transaction"+BCName, condition);
	}
	
	/** 鍖哄潡閾惧垎椤典氦鏄撴暟鎹煡璇�
	 * 
	 * @param BCName 鍖哄潡閾惧悕绉�
	 * @param condition SQL where 瀛楃涓�
	 * @param lowerlim 鍒嗛〉涓嬮檺
	 * @param upperlim 鍒嗛〉涓婇檺
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝JBCCResult.message涓篞ueryResult绫荤殑JSON搴忓垪鍗庝覆
	 * @throws TException Thrift寮傚父
	 */
	
	public JBCCResult fastSelectFromTransaction (String BCName, String condition, int limit, int offset) throws TException
	{
		return fastSelectFromBC("transaction"+BCName, condition, limit, offset);
	}
	
	
	/** 鍖哄潡閾惧缓鍧楁暟鎹煡璇�
	 * 
	 * @param BCName 鍖哄潡閾惧悕绉�
	 * @param condition SQL where 瀛楃涓�
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝JBCCResult.message涓篞ueryResult绫荤殑JSON搴忓垪鍗庝覆
	 * @throws TException Thrift寮傚父
	 */
	
	public JBCCResult fastSelectFromBlock (String BCName, String condition) throws TException
	{
		return fastSelectFromBC("block"+BCName, condition);
	}
	
	/** 鍖哄潡閾惧垎椤靛缓鍧楁暟鎹煡璇�
	 * 
	 * @param BCName 鍖哄潡閾惧悕绉�
	 * @param condition SQL where 瀛楃涓�
	 * @param lowerlim 鍒嗛〉涓嬮檺
	 * @param upperlim 鍒嗛〉涓婇檺
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝JBCCResult.message涓篞ueryResult绫荤殑JSON搴忓垪鍗庝覆
	 * @throws TException Thrift寮傚父
	 */
	
	public JBCCResult fastSelectFromBlock (String BCName, String condition, int limit, int offset) throws TException
	{
		return fastSelectFromBC("block"+BCName, condition, limit, offset);
	}
	
	
	/** 鑾峰緱浜ゆ槗鎬绘暟
	 * 
	 * @param BCName 鍖哄潡閾惧悕绉�
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝JBCCResult.message涓轰氦鏄撴�绘暟
	 * @throws TException Thrift寮傚父
	 */
	
	public JBCCResult getTransactionCount (String BCName) throws TException
	{
		return fastSelectFromBC("transaction"+BCName, "1=1");
	}
	
	/** 鑾峰緱寤哄潡鍎挎�绘暟
	 * 
	 * @param BCName 鍖哄潡閾惧悕绉�
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝JBCCResult.message涓哄缓鍧楁�绘暟
	 * @throws TException Thrift寮傚父
	 */
	
	public JBCCResult getBlockCount (String BCName) throws TException
	{
		return fastSelectFromBC("block"+BCName, "1=1");
	}
	
	
	
	/** 鍖哄潡閾惧垎椤甸�氱敤鏁版嵁鏌ヨ
	 * 
	 * @param BCName 鍖哄潡閾惧悕绉�
	 * @param condition SQL where 瀛楃涓�
	 * @param lowerlim 鍒嗛〉涓嬮檺
	 * @param upperlim 鍒嗛〉涓婇檺
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝JBCCResult.message涓篞ueryResult绫荤殑JSON搴忓垪鍗庝覆
	 * @throws TException Thrift寮傚父
	 */
	
	
	public JBCCResult fastSelectFromBC (String BCName, String condition, int limit, int offset) throws TException
	{
		JBCCResult returnresult = new JBCCResult();
		
		if (syncConnectionHashMap.isEmpty())
		{
			returnresult.setStatus(0);
			returnresult.setMessage("No open connections.");
			return returnresult;
		}
		
		if (condition.contains(";"))
		{
			returnresult.setStatus(0);
			returnresult.setMessage("Illegal conditional statement");
			return returnresult;
		}
		
		
		Random       random    = new Random();
		List<String> keys      = new ArrayList<String>(syncConnectionHashMap.keySet());
		String       randomKey = keys.get( random.nextInt(keys.size()) );
		
		
		JBCCService.Client myJBCCClient=syncConnectionHashMap.get(randomKey).get_client();
		
		if (myJBCCClient == null)
		{
			returnresult.setStatus(0);
			returnresult.setMessage("Could not retrieve Thrift client.");
			return returnresult;
		}
		
		try
        {	
            returnresult = myJBCCClient.fselectpaged(BCName, condition, limit, offset);   
        } catch (JBCCException e){
        	returnresult.setStatus(0);
			returnresult.setMessage(e.getMessage());
			return returnresult;
        }
		

		returnresult.setStatus(1);
	    return returnresult;
	
	
	}
	
	
	/** 鍖哄潡閾鹃�氱敤鏁版嵁鏌ヨ
	 * 
	 * @param BCName 鍖哄潡閾惧悕绉�
	 * @param condition SQL where 瀛楃涓�
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯革紝JBCCResult.message涓篞ueryResult绫荤殑JSON搴忓垪鍗庝覆
	 * @throws TException Thrift寮傚父
	 */
	
	public JBCCResult fastSelectFromBC (String BCName, String condition) throws TException
	{
			JBCCResult returnresult = new JBCCResult();
			
			if (syncConnectionHashMap.isEmpty())
			{
				returnresult.setStatus(0);
				returnresult.setMessage("No open connections.");
				return returnresult;
			}
			
			if (condition.contains(";"))
			{
				returnresult.setStatus(0);
				returnresult.setMessage("Illegal conditional statement");
				return returnresult;
			}
			
			
			Random       random    = new Random();
			List<String> keys      = new ArrayList<String>(syncConnectionHashMap.keySet());
			String       randomKey = keys.get( random.nextInt(keys.size()) );
			
			
			JBCCService.Client myJBCCClient=syncConnectionHashMap.get(randomKey).get_client();
			
			if (myJBCCClient == null)
			{
				returnresult.setStatus(0);
				returnresult.setMessage("Could not retrieve Thrift client.");
				return returnresult;
			}
			
			try
	        {	
	            returnresult = myJBCCClient.fselect(BCName, condition);   
	        } catch (JBCCException e){
	        	returnresult.setStatus(0);
				returnresult.setMessage(e.getMessage());
				return returnresult;
	        }
			

			returnresult.setStatus(1);
		    return returnresult;
		
	}
	
	/**鍒涘缓鍜屾敞鍐孴BC鏁版嵁缁撴瀯
	 * 
	 * @param TBCName TBC鍚嶇О
	 * @param tableDef TBC鏁版嵁缁撴瀯瀹氫箟锛屾牸寮忎负Key: 瀛楁鍚� Value: Java鍩烘湰鏁版嵁绫荤殑simple class name. 
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯�
	 * @throws TException Thrift寮傚父
	 */
	public List<JBCCResult> createTBC(String TBCName, Map<String,String> tableDef) throws TException
	{
		
		ArrayList<JBCCResult> returnresult = new ArrayList<JBCCResult>();
		JBCCResult _returnresult = new JBCCResult();
		
		if (syncConnectionHashMap.isEmpty())
		{
			_returnresult.setStatus(0);
			_returnresult.setMessage("No open connections.");
			returnresult.add(_returnresult);
			return returnresult;
		}
		
		String jsonString = JSON.toJSONString(this.maptoJSON(tableDef));
		
		/*TDLValidateResult tdlValidateResult = TDLParser.validateTableJson(ABCdef);
		
		if (!tdlValidateResult.getSuccess())
		{
			_returnresult.setStatus(0);
			_returnresult.setMessage("Table creation string invalid format.");
			returnresult.add(_returnresult);
			return returnresult;
		} */
		
		for (Map.Entry<String,  ConnectionStruct> entry : syncConnectionHashMap.entrySet()) 
		{
			
			JBCCService.Client myJBCCClient=entry.getValue().get_client();
			
			if (myJBCCClient == null)
			{
				_returnresult.setStatus(0);
				_returnresult.setMessage("Could not retrieve Thrift client.");
				returnresult.add(_returnresult);
				return returnresult;
			}
			
			try
	        {	
				JBCCResult returnresultback = myJBCCClient.createTBC(TBCName, jsonString);  
				returnresult.add(returnresultback);
	            
	        } catch (JBCCException e){
	        	_returnresult.setStatus(0);
				_returnresult.setMessage(e.getMessage());
				returnresult.add(_returnresult);
				return returnresult;
	        }
		}

	    return returnresult;
		
	}
	
	private JSONObject maptoJSON (Map<String,String> map)
	{
		JSONObject jsonObj = new JSONObject();
		for (Map.Entry<String,  String> entry : map.entrySet())
		{
			jsonObj.put(entry.getKey(), entry.getValue());
		}
		return jsonObj;
	}
	
	private JSONObject maptoJSONObject (Map<String, Object> map)
	{
		JSONObject jsonObj = new JSONObject();
		for (Map.Entry<String, Object> entry : map.entrySet())
		{
			jsonObj.put(entry.getKey(), entry.getValue());
		}
		return jsonObj;
	}
	
	/**鍒涘缓鍜屾敞鍐孴BC鏁版嵁缁撴瀯
	 * 
	 * @param ABCName ABC鍚嶇О
	 * @param tableDef TBC鏁版嵁缁撴瀯瀹氫箟锛屾牸寮忎负Key: 瀛楁鍚� Value: Java鍩烘湰鏁版嵁绫荤殑simple class name. 
	 * @return JBCCResult.status=0涓烘垚甯革紝1涓哄紓甯�
	 * @throws TException Thrift寮傚父
	 */
	public List<JBCCResult> createABC(String ABCName, Map<String,String> tableDef) throws TException
	{
			ArrayList<JBCCResult> returnresult = new ArrayList<JBCCResult>();
			JBCCResult _returnresult = new JBCCResult();
			
			if (syncConnectionHashMap.isEmpty())
			{
				_returnresult.setStatus(0);
				_returnresult.setMessage("No open connections.");
				returnresult.add(_returnresult);
				return returnresult;
			}
			
			String jsonString = JSON.toJSONString(this.maptoJSON(tableDef));
			
			/*TDLValidateResult tdlValidateResult = TDLParser.validateTableJson(ABCdef);
			
			if (!tdlValidateResult.getSuccess())
			{
				_returnresult.setStatus(0);
				_returnresult.setMessage("Table creation string invalid format.");
				returnresult.add(_returnresult);
				return returnresult;
			} */
			
			for (Map.Entry<String,  ConnectionStruct> entry : syncConnectionHashMap.entrySet()) 
			{
				
				JBCCService.Client myJBCCClient=entry.getValue().get_client();
				
				if (myJBCCClient == null)
				{
					_returnresult.setStatus(0);
					_returnresult.setMessage("Could not retrieve Thrift client.");
					returnresult.add(_returnresult);
					return returnresult;
				}
				
				try
		        {	
					JBCCResult returnresultback = myJBCCClient.createABC(ABCName, jsonString);  
					returnresult.add(returnresultback);
		            
		        } catch (JBCCException e){
		        	_returnresult.setStatus(0);
					_returnresult.setMessage(e.getMessage());
					returnresult.add(_returnresult);
					return returnresult;
		        }
			}

		    return returnresult;
		
	}
	
	
	
	/** 鑾峰緱褰撳墠Client绔渶楂樺紓姝ュ疄渚嬫暟
	 * 
	 * @return 褰撳墠Client绔渶楂樺紓姝ュ疄渚嬫暟
	 */
	
	public int getDEFAULTMAXCLIENT() {
		return DEFAULTMAXCLIENT;
	}

	/** 璁剧疆褰撳墠Client绔渶楂樺紓姝ュ疄渚嬫暟
	 * 
	 * @param dEFAULTMAXCLIENT 褰撳墠Client绔渶楂樺紓姝ュ疄渚嬫暟
	 */
	public void setDEFAULTMAXCLIENT(int dEFAULTMAXCLIENT) {
		DEFAULTMAXCLIENT = dEFAULTMAXCLIENT;
	}

	/** 鑾峰緱褰撳墠Client绔渶楂樺紓姝ョ嚎鏁�
	 * 
	 * @return 褰撳墠Client绔渶楂樺紓姝ョ嚎鏁�
	 */
	public int getDEFAULTMAXCLIENTPERTHREAD() {
		return DEFAULTMAXCLIENTPERTHREAD;
	}

	/** 璁剧疆褰撳墠Client绔渶楂樺紓姝ョ嚎鏁�
	 * 
	 * @param dEFAULTMAXCLIENTPERTHREAD 褰撳墠Client绔渶楂樺紓姝ョ嚎鏁�
	 */
	public void setDEFAULTMAXCLIENTPERTHREAD(int dEFAULTMAXCLIENTPERTHREAD) {
		DEFAULTMAXCLIENTPERTHREAD = dEFAULTMAXCLIENTPERTHREAD;
	}
}
