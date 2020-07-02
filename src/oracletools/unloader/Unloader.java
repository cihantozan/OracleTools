package oracletools.unloader;

import java.io.IOException;
import java.sql.SQLException;

import oracletools.util.Logger;
import oracletools.util.MultithreadMessaging;
import oracletools.util.OracleConnection;
import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;

public class Unloader {
	
	private OracleConnection connection;
	private String file;
	private String query; 
	private String columnDelimiter; 
	private String rowDelimiter;
	private boolean addColumnNames; 
	private String dateFormat;
	private String dateTimeFormat; 
	private char decimalSeperator; 
	private int fetchSize;
	private int rowCountMessageLength;
	private int parallelCount;
	private String[] parallelDivisorColumns;

	public Unloader(OracleConnection connection, String file, String query, String columnDelimiter, String rowDelimiter,boolean addColumnNames, String dateFormat, String dateTimeFormat, char decimalSeperator, int fetchSize, int rowCountMessageLength, int parallelCount, String[] parallelDivisorColumns) {
		super();
		this.connection = connection;
		this.file = file;
		this.query = query;
		this.columnDelimiter = columnDelimiter;
		this.rowDelimiter = rowDelimiter;
		this.addColumnNames = addColumnNames;
		this.dateFormat = dateFormat;
		this.dateTimeFormat = dateTimeFormat;
		this.decimalSeperator = decimalSeperator;
		this.fetchSize = fetchSize;
		this.rowCountMessageLength=rowCountMessageLength;
		this.parallelCount=parallelCount;
		this.parallelDivisorColumns=parallelDivisorColumns;
	}
	
	
	
	public void unload() throws ClassNotFoundException, SQLException, IOException {
	
		for(int i=0;i<parallelCount;i++) {
			String threadName="SerialUnloader"+i;
			SerialUnloader serialUnloader=new SerialUnloader(threadName,connection, file, query, columnDelimiter, rowDelimiter, addColumnNames, dateFormat, dateTimeFormat, decimalSeperator, fetchSize, rowCountMessageLength);
						
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
			
			serialUnloader.run();
		}	
		
	}

}
