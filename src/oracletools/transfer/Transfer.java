package oracletools.transfer;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import oracletools.util.ConfigReader;
import oracletools.util.Logger;
import oracletools.util.MultithreadMessaging;
import oracletools.util.OracleConnection;
import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;

public class Transfer {

	private TransferParameters parameters;
	private Thread[] threads;
	
	
	
	public Transfer() throws ParserConfigurationException, SAXException, IOException {	
		super();
		ConfigReader configReader=new ConfigReader();
		Map<String, String> xmlParameters = configReader.read("transferConfig.xml");
		
		this.parameters=new TransferParameters();
		parameters.setSourceConnection(new OracleConnection(xmlParameters.get("sourceDbUser"), xmlParameters.get("sourceDbPassword"), xmlParameters.get("sourceDbHost"), Integer.parseInt(xmlParameters.get("sourceDbPort")), xmlParameters.get("sourceDbName")));
		parameters.setTargetConnection(new OracleConnection(xmlParameters.get("targetDbUser"), xmlParameters.get("targetDbPassword"), xmlParameters.get("targetDbHost"), Integer.parseInt(xmlParameters.get("targetDbPort")), xmlParameters.get("targetDbName")));
		parameters.setSourceQuery(xmlParameters.get("sourceQuery"));
		parameters.setTargetTable(xmlParameters.get("targetTable"));
		parameters.setBatchSize(Integer.parseInt(xmlParameters.get("batchSize")));
		parameters.setTruncateTargetTable(Boolean.parseBoolean(xmlParameters.get("truncateTargetTable")));
		parameters.setDirectPathInsert(Boolean.parseBoolean(xmlParameters.get("directPathInsert")));
		parameters.setCommitAfterLoad(Boolean.parseBoolean(xmlParameters.get("commitAfterLoad")));
		parameters.setParallelCount(Integer.parseInt(xmlParameters.get("parallelCount")));
		parameters.setParallelDivisorColumns(xmlParameters.get("parallelDivisorColumns").split(","));
		
		this.threads=new Thread[this.parameters.getParallelCount()];
	}

	public Transfer(TransferParameters parameters) {
		super();
		this.parameters = parameters;	
		this.threads=new Thread[this.parameters.getParallelCount()];
	}
	
	public TransferParameters getParameters() {
		return parameters;
	}
	public void setParameters(TransferParameters parameters) {
		this.parameters = parameters;
	}
	public Thread[] getThreads() {
		return threads;
	}
	public void setThreads(Thread[] threads) {
		this.threads = threads;
	}
	
public void transfer() throws InterruptedException {
		
		if(this.parameters.isDirectPathInsert() && this.parameters.getParallelCount()>1) {
			this.parameters.setDirectPathInsert(false);
			MultithreadMessaging.printMessage("Direct Path Insert is disabled because parallel count="+this.parameters.getParallelCount());
		}
		
		if(this.parameters.getParallelCount()>1 && this.parameters.isTruncateTargetTable()) {
			SerialTransfer serialTransfer = new SerialTransfer("Truncate", parameters, 1);
			serialTransfer.setErrorListener(new ErrorListener() {				
				@Override
				public void onError(String threadName, Exception e) {
					MultithreadMessaging.onThreadError(threadName,e);
					
				}
			});
			
			serialTransfer.setLoggerActivityListener(new LoggerActivityListener() {				
				@Override
				public void onLogActivity(String threadName, Logger logger) {
					MultithreadMessaging.onThreadLogActivity(threadName, logger);
				}
			});
			serialTransfer.truncateTable();
		}
		MultithreadMessaging.reset();
		
		for(int i=0;i<this.parameters.getParallelCount();i++) {
			
			String name="Transfer"+(i+1);
			SerialTransfer serialTransfer=new SerialTransfer(name, parameters, (i+1));
						
			serialTransfer.setErrorListener(new ErrorListener() {				
				@Override
				public void onError(String threadName, Exception e) {
					MultithreadMessaging.onThreadError(threadName,e);
					
				}
			});
			
			serialTransfer.setLoggerActivityListener(new LoggerActivityListener() {				
				@Override
				public void onLogActivity(String threadName, Logger logger) {
					MultithreadMessaging.onThreadLogActivity(threadName, logger);
				}
			});
			
			threads[i]=new Thread(serialTransfer);			
			threads[i].start();			
		}
		for(int i=0;i<this.parameters.getParallelCount();i++) {
			threads[i].join();
		}
		
	}
	
}
