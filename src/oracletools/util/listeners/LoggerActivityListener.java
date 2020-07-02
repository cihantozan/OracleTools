package oracletools.util.listeners;

import oracletools.util.Logger;

public interface LoggerActivityListener {	
	void onLogActivity(String threadName, Logger logger);	
}