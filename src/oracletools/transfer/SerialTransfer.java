package oracletools.transfer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import oracletools.util.IOracleTool;
import oracletools.util.IParameters;
import oracletools.util.Logger;
import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;

public class SerialTransfer implements IOracleTool {
	
	private String name;
	@Override
	public String getName() {
		return this.name;
	}
	@Override
	public void SetName(String name) {
		this.name=name;		
	}
	

	private TransferParameters parameters;
	@Override
	public IParameters getParameters() {		
		return this.parameters;
	}
	@Override
	public void setParameters(IParameters parameters) {
		this.parameters=(TransferParameters) parameters;
	}
	
	private int parallelOrder;	
	public int getParallelOrder() {
		return parallelOrder;
	}
	public void setParallelOrder(int parallelOrder) {
		this.parallelOrder = parallelOrder;
	}
	
	
	private ErrorListener errorListener;			
	public ErrorListener getErrorListener() {
		return errorListener;
	}
	public void setErrorListener(ErrorListener errorListener) {
		this.errorListener = errorListener;
	}

		
	public LoggerActivityListener getLoggerActivityListener() {
		return logger.getLoggerActivityListener();
	}
	public void setLoggerActivityListener(LoggerActivityListener loggerActivityListener) {	
		logger.setLoggerActivityListener(loggerActivityListener);
	}
	
	
	
	private Connection sourceCon;
	private Connection targetCon;	
	
	private Logger logger;
	
	public SerialTransfer(String name, IParameters parameters, int parallelOrder) {
		super();
		this.name=name;
		this.parameters=(TransferParameters)parameters;
		this.parallelOrder=parallelOrder;
		
		logger=new Logger(name);
	}
	
	public void truncateTable() {
		Connection cont=null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			cont = DriverManager.getConnection(this.parameters.getTargetConnection().getConnectionString(),
					this.parameters.getTargetConnection().getUser(), this.parameters.getTargetConnection().getPassword());
			Statement stmt = cont.createStatement();
			stmt.executeUpdate("truncate table " + this.parameters.getTargetTable());
			cont.close();
			logger.message("Truncated");
		} catch (Exception e) {
			try {
				cont.close();				
			} catch (Exception e1) {
			}

			logger.error();
			errorListener.onError(this.name, e);
		}
	}
	
	public void transfer() {
		
		try {
		
			logger.start();
		
			//run source query
			
			String query=this.parameters.getSourceQuery();
			String columns="''||";
			for(int i=0; i<this.parameters.getParallelDivisorColumns().length; i++) {
				columns+=this.parameters.getParallelDivisorColumns()[i];
				if(i != this.parameters.getParallelDivisorColumns().length-1) {
					columns+="||";
				}
			}
			query="select * from ( " + query + " ) where ora_hash("+columns+","+(this.parameters.getParallelCount()-1)+")="+ (this.parallelOrder-1);
			
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			sourceCon=DriverManager.getConnection(this.parameters.getSourceConnection().getConnectionString(),this.parameters.getSourceConnection().getUser(),this.parameters.getSourceConnection().getPassword());
			Statement sourceStmt=sourceCon.createStatement();
			sourceStmt.setFetchSize(this.parameters.getBatchSize());			
			ResultSet rs=sourceStmt.executeQuery(query);
			rs.setFetchSize(this.parameters.getBatchSize());
			ResultSetMetaData resultSetMetaData = rs.getMetaData();
			
			//target connection
			Class.forName("oracle.jdbc.driver.OracleDriver");
			targetCon=DriverManager.getConnection(this.parameters.getTargetConnection().getConnectionString(),this.parameters.getTargetConnection().getUser(),this.parameters.getTargetConnection().getPassword());		
			String queryParameters="?,".repeat(resultSetMetaData.getColumnCount());
			queryParameters=queryParameters.substring(0,queryParameters.length()-1);		
			String append="";
			if(this.parameters.isDirectPathInsert()) append="/*+ append_values */ ";
			String insertQueryString="insert " + append + "into " + this.parameters.getTargetTable() + " values (" + queryParameters + ")";
			PreparedStatement insertQuery=targetCon.prepareStatement(insertQueryString);
			
			if(this.parameters.isDirectPathInsert()) {
				targetCon.setAutoCommit(true);
			}
			else {
				targetCon.setAutoCommit(false);
			}
			
			
			if(this.parameters.isTruncateTargetTable() && this.parameters.getParallelCount()==1) {
				truncateTable();
			}
			
			
			logger.message("Query executed");
			
			
			
			//transfer
			
			int currentBatchSize=0;			
			
			Object[] row=new Object[resultSetMetaData.getColumnCount()];
			while(rs.next()) {
				
				//fetch row
				for(int i=0;i<resultSetMetaData.getColumnCount();i++) {
					row[i]=rs.getObject(i+1);
				}
				
				//prepare one row query and add to batch
				for(int i=0;i<resultSetMetaData.getColumnCount();i++) {
					insertQuery.setObject(i+1, row[i]);
				}
				insertQuery.addBatch();
				currentBatchSize++;
				
				//if ready, run batch
				if(currentBatchSize==this.parameters.getBatchSize()) {
					insertQuery.executeBatch();				
					if(!this.parameters.isDirectPathInsert() && !this.parameters.isCommitAfterLoad()) {
						targetCon.commit();
					}
					
					logger.step(currentBatchSize, "rows transferred");				
					
					currentBatchSize=0;
				}
							
			}
			
			//remaining rows
			if(currentBatchSize>0) {
				insertQuery.executeBatch();
				if(!this.parameters.isDirectPathInsert() && !this.parameters.isCommitAfterLoad()) {
					targetCon.commit();
				}				
				
				logger.step(currentBatchSize, "rows transferred");
			}
			
			if(this.parameters.isCommitAfterLoad()) {
				targetCon.commit();
			}
			sourceCon.close();
			targetCon.close();
			
			logger.end();						
		
		}
		catch(Exception e) {
			try {
				sourceCon.close();
				targetCon.close();
			}
			catch(Exception e1) {}
			
			logger.error();
			errorListener.onError(this.name,e);	
		}
		
		
	}
	
	public void run() {
		transfer();
	}

	
	
}
