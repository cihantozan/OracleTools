package oracletools.transfer;

import oracletools.util.Logger;
import oracletools.util.MultithreadMessaging;
import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;

public class Transfer {

	private TransferParameters parameters;
	private Thread[] threads;
	
	
	
	public Transfer() {		
	}

	public Transfer(TransferParameters parameters, Thread[] threads) {
		super();
		this.parameters = parameters;
		this.threads = threads;
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
		
		if(this.parameters.getParallelCount()>0) {
			this.parameters.setDirectPathInsert(false);
		}
		
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
