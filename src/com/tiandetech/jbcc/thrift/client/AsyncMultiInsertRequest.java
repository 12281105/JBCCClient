package com.tiandetech.jbcc.thrift.client;

import java.util.ArrayList;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCResult;
import com.tiandetech.blockchain.thrift.blockchainNetwork.JBCCService.AsyncClient;

public class AsyncMultiInsertRequest extends AsyncConnection.ThriftRequest {

	private String _TBCName;
	private ArrayList<JBCCResult> _results;
	private JBCCClient _myJBCCClient;
	private Map<String, String> _transactmap;
	private int _delay;
	
	
	
	public ArrayList<JBCCResult> get_results() {
		return _results;
	}

	public void set_results(ArrayList<JBCCResult> _results) {
		this._results = _results;
	}

	public JBCCClient get_myJBCCClient() {
		return _myJBCCClient;
	}

	public void set_myJBCCClient(JBCCClient _myJBCCClient) {
		this._myJBCCClient = _myJBCCClient;
	}

	public Map<String, String> get_transactmap() {
		return _transactmap;
	}

	public void set_transactmap(Map<String, String> _transactmap) {
		this._transactmap = _transactmap;
	}

	public int get_delay() {
		return _delay;
	}

	public void set_delay(int _delay) {
		this._delay = _delay;
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
            cli.multiinsert(_TBCName, _transactmap, new AsyncMethodCallback<AsyncClient.multiinsert_call>() {

                @Override
                public void onComplete(AsyncClient.multiinsert_call response) {
                    try {
                        System.out.println("AsynCall MultiInsert Result =:" + response.getResult());
                        
                        try {
							Thread.sleep(1000*_delay);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                        if(_transactmap.keySet().iterator().hasNext())
                        {
                        	JBCCResult thisresult = _myJBCCClient.fastSelectFromBC("transaction"+_TBCName, "uuid="+_transactmap.keySet().iterator().next());
                        	_results.add(thisresult);
                        }
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
		return _results;
	}

	public void setResults(ArrayList<JBCCResult> results) {
		this._results = results;
	}
	
}