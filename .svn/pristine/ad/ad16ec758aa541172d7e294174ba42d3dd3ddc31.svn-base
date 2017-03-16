package com.tiandetech.jbcc.thrift.client;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService.Client;

public class ConnectionStruct {
	
	private JBCCService.Client _client;
	private TTransport _transport;
	private TProtocol _protocal;
	
	public ConnectionStruct(Client _client, TTransport _transport, TProtocol _protocal) {
		
		this._client = _client;
		this._transport = _transport;
		this._protocal = _protocal;
	}
	
	
	public JBCCService.Client get_client() {
		return _client;
	}
	public void set_client(JBCCService.Client _client) {
		this._client = _client;
	}
	public TTransport get_transport() {
		return _transport;
	}
	public void set_transport(TTransport _transport) {
		this._transport = _transport;
	}
	public TProtocol get_protocal() {
		return _protocal;
	}
	public void set_protocal(TProtocol _protocal) {
		this._protocal = _protocal;
	}
	
	

}
