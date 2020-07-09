package oracletools.util;

import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;

public interface IOracleTool extends Runnable {
	String getName();
	void SetName(String name);
	
	IParameters getParameters();
	void setParameters(IParameters parameters);
	
	int getParallelOrder();
	void setParallelOrder(int parallelOrder);
	
	ErrorListener getErrorListener();
	void setErrorListener(ErrorListener errorListener);
	
	LoggerActivityListener getLoggerActivityListener();
	void setLoggerActivityListener(LoggerActivityListener loggerActivityListener);
}
