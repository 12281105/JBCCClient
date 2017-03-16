package com.tiandetech.jbcc.thrift.client;

import java.util.List;
import java.util.Map;

public class QueryResult {

	List<Map<String,Object>> resultlist;

	public List<Map<String, Object>> getResultlist() {
		return resultlist;
	}

	public void setResultlist(List<Map<String, Object>> resultlist) {
		this.resultlist = resultlist;
	}
	
	
	
}
