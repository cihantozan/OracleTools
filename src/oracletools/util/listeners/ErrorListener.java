package oracletools.util.listeners;

public interface ErrorListener {
	void onError(String threadName, Exception e);
}