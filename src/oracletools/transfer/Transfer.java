package oracletools.transfer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import oracletools.util.Logger;
import oracletools.util.OracleConnection;

public class Transfer {
	

	private OracleConnection sourceConnection;
	private OracleConnection targetConnection;
	private String sourceQuery;
	private String targetTable;
	private int batchSize;	
	private boolean truncateTargetTable;
	private boolean directPathInsert;
	private boolean commitAfterLoad;
	
	private Connection sourceCon;
	private Connection targetCon;	
	
	private Logger logger;
	
	public Transfer(OracleConnection sourceConnection, OracleConnection targetConnection, String sourceQuery, String targetTable, int batchSize, boolean truncateTargetTable, boolean directPathInsert, boolean commitAfterLoad) {
		super();
		this.sourceConnection = sourceConnection;
		this.targetConnection = targetConnection;
		this.sourceQuery = sourceQuery;
		this.targetTable = targetTable;
		this.batchSize = batchSize;
		this.truncateTargetTable = truncateTargetTable;
		this.directPathInsert = directPathInsert;
		this.commitAfterLoad=commitAfterLoad;
		
		logger=new Logger();
	}
	
	public void transfer() throws ClassNotFoundException, SQLException {
		
		try {
		
			logger.start();
		
			//run source query
			Class.forName("oracle.jdbc.driver.OracleDriver");
			sourceCon=DriverManager.getConnection(sourceConnection.getConnectionString(),sourceConnection.getUser(),sourceConnection.getPassword());
			Statement sourceStmt=sourceCon.createStatement();
			sourceStmt.setFetchSize(batchSize);			
			ResultSet rs=sourceStmt.executeQuery(sourceQuery);
			rs.setFetchSize(batchSize);
			ResultSetMetaData resultSetMetaData = rs.getMetaData();
			
			//target connection
			Class.forName("oracle.jdbc.driver.OracleDriver");
			targetCon=DriverManager.getConnection(targetConnection.getConnectionString(),targetConnection.getUser(),targetConnection.getPassword());		
			String parameters="?,".repeat(resultSetMetaData.getColumnCount());
			parameters=parameters.substring(0,parameters.length()-1);		
			String append="";
			if(directPathInsert) append="/*+ append_values */ ";
			String insertQueryString="insert " + append + "into " + targetTable + " values (" + parameters + ")";
			PreparedStatement insertQuery=targetCon.prepareStatement(insertQueryString);
			
			if(directPathInsert) {
				targetCon.setAutoCommit(true);
			}
			else {
				targetCon.setAutoCommit(false);
			}
			
			
			if(truncateTargetTable) {
				Statement truncateStmt=targetCon.createStatement();
				truncateStmt.executeUpdate("truncate table " + targetTable);
				logger.message("Truncated");
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
				if(currentBatchSize==batchSize) {
					insertQuery.executeBatch();				
					if(!directPathInsert && !commitAfterLoad) {
						targetCon.commit();
					}
					
					logger.step(currentBatchSize, "rows transferred");				
					
					currentBatchSize=0;
				}
							
			}
			
			//remaining rows
			if(currentBatchSize>0) {
				insertQuery.executeBatch();
				if(!directPathInsert && !commitAfterLoad) {
					targetCon.commit();
				}				
				
				logger.step(currentBatchSize, "rows transferred");
			}
			
			if(commitAfterLoad) {
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
			
			throw e;
		}
		
		
	}

	
	
}
