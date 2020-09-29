package oracletools.unloader;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import oracletools.util.ConfigReader;
import oracletools.util.FileCombiner;
import oracletools.util.IParameters;
import oracletools.util.Logger;
import oracletools.util.MultithreadMessaging;
import oracletools.util.OracleConnection;
import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;

public class Unloader {
	
	private UnloaderParameters parameters;	
	private Thread[] threads;
	
		
	public IParameters getParameters() {
		return parameters;
	}
	public void setParameters(IParameters parameters) {
		this.parameters = (UnloaderParameters)parameters;
	}
	


	public Unloader(IParameters parameters) {
		super();
		this.parameters=(UnloaderParameters)parameters;		
		this.threads=new Thread[this.parameters.getParallelCount()];
	}
	public Unloader() throws ParserConfigurationException, SAXException, IOException {
		ConfigReader configReader=new ConfigReader();
		Map<String, String> xmlParameters = configReader.read("unloaderConfig.xml");	
		
		parameters=new UnloaderParameters();
		parameters.setConnection(new OracleConnection(xmlParameters.get("dbUser"), xmlParameters.get("dbPassword"), xmlParameters.get("dbHost"), Integer.parseInt(xmlParameters.get("dbPort")), xmlParameters.get("dbName")));
		parameters.setFile(xmlParameters.get("file"));
		parameters.setQuery(xmlParameters.get("query"));
		parameters.setColumnDelimiter(xmlParameters.get("columnDelimiter"));
		parameters.setRowDelimiter(xmlParameters.get("rowDelimiter"));
		parameters.setAddColumnNames(Boolean.parseBoolean(xmlParameters.get("addColumnNames")));
		parameters.setDateFormat(xmlParameters.get("dateFormat"));
		parameters.setDateTimeFormat(xmlParameters.get("dateTimeFormat"));
		parameters.setDecimalSeperator(xmlParameters.get("decimalSeperator").charAt(0));
		parameters.setFetchSize(Integer.parseInt(xmlParameters.get("fetchSize")));
		parameters.setRowCountMessageLength(Integer.parseInt(xmlParameters.get("rowCountMessageLength")));
		parameters.setParallelCount(Integer.parseInt(xmlParameters.get("parallelCount")));
		parameters.setParallelDivisorColumns(xmlParameters.get("parallelDivisorColumns").split(","));
		parameters.setCombineFiles(Boolean.parseBoolean(xmlParameters.get("combineFiles")));
		parameters.setFileType(FileType.valueOf(xmlParameters.get("fileType")));
		
		this.threads=new Thread[this.parameters.getParallelCount()];
		
	}	
	
	
	public void unload() throws Exception {
	
		for(int i=0;i<this.parameters.getParallelCount();i++) {
									
			String name="SerialUnloader"+(i+1);
			SerialUnloader serialUnloader=new SerialUnloader(name, parameters, i+1 );
						
			serialUnloader.setErrorListener(new ErrorListener() {				
				@Override
				public void onError(String threadName, Exception e) {
					MultithreadMessaging.onThreadError(threadName,e);
					
				}
			});
			
			serialUnloader.setLoggerActivityListener(new LoggerActivityListener() {				
				@Override
				public void onLogActivity(String threadName, Logger logger) {
					MultithreadMessaging.onThreadLogActivity(threadName, logger);
				}
			});
			
			threads[i]=new Thread(serialUnloader);			
			threads[i].start();			
		}
		for(int i=0;i<this.parameters.getParallelCount();i++) {
			threads[i].join();
		}
		
		
		
		if(this.parameters.getParallelCount()>1 && this.parameters.isCombineFiles()) {
			MultithreadMessaging.reset();
			
			String[] files=new String[this.parameters.getParallelCount()];
			for(int i=0;i<this.parameters.getParallelCount();i++) {				
				String fileName=this.parameters.getFile();				
				files[i] = fileName.substring(0, fileName.lastIndexOf("."))  + "_" + (i+1) + fileName.substring(fileName.lastIndexOf("."));
			}
			FileCombiner fileCombiner=new FileCombiner(files, this.parameters.getFile(), this.parameters.getRowDelimiter());
			
			fileCombiner.setLoggerActivityListener(new LoggerActivityListener() {
				
				@Override
				public void onLogActivity(String threadName, Logger logger) {
					MultithreadMessaging.onThreadLogActivity(threadName, logger);					
				}
			});
			
			fileCombiner.combine();
		}
		
	}

}
