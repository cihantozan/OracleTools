package oracletools.unloader;

import oracletools.util.FileCombiner;
import oracletools.util.IParameters;
import oracletools.util.Logger;
import oracletools.util.MultithreadMessaging;
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
