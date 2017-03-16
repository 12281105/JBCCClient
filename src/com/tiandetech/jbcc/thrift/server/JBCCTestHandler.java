package com.tiandetech.jbcc.thrift.server;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService;

public class JBCCTestHandler  {

    public static void main(String args[])
                    throws Exception
    {
    		
            TServerTransport transport=new TServerSocket(9090);
            TServer server=new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(new JBCCService.Processor(new JBCCServiceImpl())));
            System.out.println("Started");
            server.serve();
    }

	
}
