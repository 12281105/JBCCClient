package com.tiandetech.jbcc.thrift.server;

import java.util.Map;

import org.apache.thrift.TException;

import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCException;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCResult;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService;

public class JBCCServiceImpl implements JBCCService.Iface {

	@Override
	public JBCCResult insert(String content, String TBCName) throws JBCCException, TException {
		JBCCResult returnresult = new JBCCResult();
		returnresult.setStatus(1);
		returnresult.setMessage(content);
		System.out.println("insert method hit: "+ content + ":" + TBCName);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return returnresult;
	}

	@Override
	public JBCCResult select(String TBCName, String condition) throws JBCCException, TException {
		JBCCResult returnresult = new JBCCResult();
		returnresult.setStatus(1);
		returnresult.setMessage(condition);
		System.out.println("select method hit: "+ condition + ":" + TBCName);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return returnresult;
	}

	@Override
	public JBCCResult fselect(String TBCName, String condition) throws JBCCException, TException {
		JBCCResult returnresult = new JBCCResult();
		System.out.println(TBCName + ":" + condition);
		
		
		
		return returnresult;
	}

	@Override
	public JBCCResult createTBC(String TBCdef, String TBCName) throws JBCCException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JBCCResult createABC(String ABCdef, String ABCName) throws JBCCException, TException {
		System.out.println(ABCdef);
		JBCCResult returnresult = new JBCCResult();
		return returnresult;
	}

	@Override
	public JBCCResult finsert(String TBCName, String content) throws JBCCException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JBCCResult multiinsert(String TBCName, Map<String, String> transactions) throws JBCCException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JBCCResult fselectpaged(String TBCName, String condition, int limit, int offset)
			throws JBCCException, TException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
