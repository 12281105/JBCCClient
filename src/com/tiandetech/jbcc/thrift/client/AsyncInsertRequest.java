package com.tiandetech.jbcc.thrift.client;

import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCResult;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService.AsyncClient;

public class AsyncInsertRequest extends AsyncConnection.ThriftRequest {

	private String _content;
	private String _TBCName;
	private ArrayList<JBCCResult> results;
	
	
	
	
    public String get_content() {
		return _content;
	}

	public void set_content(String _content) {
		this._content = _content;
	}

	public String get_TBCName() {
		return _TBCName;
	}

	public void set_TBCName(String _TBCName) {
		this._TBCName = _TBCName;
	}

	@Override
    public void on(AsyncClient cli) {
        try {
            cli.insert(_TBCName, _content, new AsyncMethodCallback<AsyncClient.insert_call>() {

                @Override
                public void onComplete(AsyncClient.insert_call response) {
                    try {
                        System.out.println("AsynCall Insert Result =:" + response.getResult());
                        results.add(response.getResult());
                    } catch (TException e) {
                        e.printStackTrace();
                    } finally {
                        //latch.countDown();
                    }
                }

                @Override
                public void onError(Exception e) {
                    System.out.println("onError :" + e.getMessage());
                    //latch.countDown();
                }
            });
        } catch (TException e) {
            e.printStackTrace();
        }
    }

	public ArrayList<JBCCResult> getResults() {
		return results;
	}

	public void setResults(ArrayList<JBCCResult> results) {
		this.results = results;
	}
	
}
