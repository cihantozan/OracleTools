package oracletools.loader;

import oracletools.util.IParameters;
import oracletools.util.Logger;
import oracletools.util.MultithreadMessaging;
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
	
	public void load() throws InterruptedException {
		
		if(this.parameters.getParallelCount()>0) {
			this.parameters.setDirectPathInsert(false);
		}
		
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
