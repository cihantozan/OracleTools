package oracletools.util;

public interface IOracleTool extends Runnable {
	String getName();
	void SetName(String name);
	
	IParameters getParameters();
	void setParameters(IParameters parameters);
}
