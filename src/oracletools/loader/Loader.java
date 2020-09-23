package oracletools.loader;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import oracletools.util.ConfigReader;
import oracletools.util.IParameters;
import oracletools.util.Logger;
import oracletools.util.MultithreadMessaging;
import oracletools.util.OracleConnection;
import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;

public class Loader {
	private LoaderParameters parameters;
	private Thread[] threads;
		
	public LoaderParameters getParameters() {
		return parameters;
	}
	public void setParameters(IParameters parameters) {
		this.parameters = (LoaderParameters)parameters;
	}
	
	
	public Loader(IParameters parameters) {
		super();
		this.parameters=(LoaderParameters)parameters;
		this.threads=new Thread[this.parameters.getParallelCount()];
	}
	
	public Loader() throws ParserConfigurationException, SAXException, IOException {
		super();
		ConfigReader configReader=new ConfigReader();
		Map<String, String> xmlParameters = configReader.read("loaderConfig.xml");
		
		this.parameters=new LoaderParameters();
		parameters.setConnection(new OracleConnection(xmlParameters.get("dbUser"), xmlParameters.get("dbPassword"), xmlParameters.get("dbHost"), Integer.parseInt(xmlParameters.get("dbPort")), xmlParameters.get("dbName")));
		parameters.setFile(xmlParameters.get("file"));
		parameters.setTableName(xmlParameters.get("tableName"));
		parameters.setColumnDelimiter(xmlParameters.get("columnDelimiter"));
		parameters.setRowDelimiter(xmlParameters.get("rowDelimiter"));
		parameters.setSkipRowCount(Integer.parseInt(xmlParameters.get("skipRowCount")));
		parameters.setDateTimeFormat(xmlParameters.get("dateTimeFormat"));
		parameters.setTimestampFormat(xmlParameters.get("timestampFormat"));
		parameters.setDecimalSeperator(xmlParameters.get("decimalSeperator").toCharArray()[0]);
		parameters.setBatchSize(Integer.parseInt(xmlParameters.get("batchSize")));
		parameters.setTruncateTargetTable(Boolean.parseBoolean(xmlParameters.get("truncateTargetTable")));
		parameters.setDirectPathInsert(Boolean.parseBoolean(xmlParameters.get("directPathInsert")));
		parameters.setCommitAfterLoad(Boolean.parseBoolean(xmlParameters.get("commitAfterLoad")));
		parameters.setParallelCount(Integer.parseInt(xmlParameters.get("parallelCount")));
		
		this.threads=new Thread[this.parameters.getParallelCount()];
	}
	
	
	public void load() throws InterruptedException {
		
		if(this.parameters.isDirectPathInsert() && this.parameters.getParallelCount()>1) {
			this.parameters.setDirectPathInsert(false);
			MultithreadMessaging.printMessage("Direct Path Insert is disabled because parallel count="+this.parameters.getParallelCount());
		}
		
		if(this.parameters.getParallelCount()>1 && this.parameters.isTruncateTargetTable()) {
			SerialLoader serialLoader = new SerialLoader("Truncate", parameters, 1);
			serialLoader.setErrorListener(new ErrorListener() {				
				@Override
				public void onError(String threadName, Exception e) {
					MultithreadMessaging.onThreadError(threadName,e);
					
				}
			});
			
			serialLoader.setLoggerActivityListener(new LoggerActivityListener() {				
				@Override
				public void onLogActivity(String threadName, Logger logger) {
					MultithreadMessaging.onThreadLogActivity(threadName, logger);
				}
			});
			serialLoader.truncateTable();
		}
		MultithreadMessaging.reset();
		
		for(int i=0;i<this.parameters.getParallelCount();i++) {
			
			String name="SerialLoader"+(i+1);
			SerialLoader serialLoader=new SerialLoader(name, parameters, (i+1));
						
			serialLoader.setErrorListener(new ErrorListener() {				
				@Override
				public void onError(String threadName, Exception e) {
					MultithreadMessaging.onThreadError(threadName,e);
					
				}
			});
			
			serialLoader.setLoggerActivityListener(new LoggerActivityListener() {				
				@Override
				public void onLogActivity(String threadName, Logger logger) {
					MultithreadMessaging.onThreadLogActivity(threadName, logger);
				}
			});
			
			threads[i]=new Thread(serialLoader);			
			threads[i].start();			
		}
		for(int i=0;i<this.parameters.getParallelCount();i++) {
			threads[i].join();
		}
		
	}
	
}
