package com.tiandetech.jbcc.thrift.server;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService.Processor;


public class JBCCNonBlockingHandler {
	public final static int PORT = 8989;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void start() {
		try {
			TNonblockingServerSocket socket = new TNonblockingServerSocket(PORT);
			final JBCCService.Processor processor = new JBCCService.Processor(new JBCCServiceImpl());
			THsHaServer.Args arg = new THsHaServer.Args(socket);
			arg.protocolFactory(new TCompactProtocol.Factory());
			arg.transportFactory(new TFramedTransport.Factory());
			arg.processorFactory(new TProcessorFactory(processor));
			TServer server = new THsHaServer(arg);
			System.out.println("Nonblocking server started...");
			server.serve();

		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		JBCCNonBlockingHandler srv = new JBCCNonBlockingHandler();
		srv.start();
	}
}