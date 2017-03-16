package com.tiandetech.jbcc.thrift.server;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService;

public class JBCCSecureHandler {
	 
    private void start() {
        try {
            TSSLTransportFactory.TSSLTransportParameters params =
                    new TSSLTransportFactory.TSSLTransportParameters();
            params.setKeyStore("/Users/Wei/Documents/workspace/keystore.jks", "cerio1");
 
            TServerSocket serverTransport = TSSLTransportFactory.getServerSocket(
                    7911, 10000, InetAddress.getByName("localhost"), params);
            JBCCService.Processor processor = new JBCCService.Processor(new JBCCServiceImpl());
 
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).
                    processor(processor));
            System.out.println("Starting server on port 7911 ...");
            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
 
        }
    }
 
    public static void main(String[] args) {
    	JBCCSecureHandler srv = new JBCCSecureHandler();
        srv.start();
    }
}