package oracletools.loader;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Scanner;

import oracletools.util.IOracleTool;
import oracletools.util.IParameters;
import oracletools.util.Logger;
import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;

public class SerialLoader implements IOracleTool {

	private String name;
	private int parallelOrder;
	

	private LoaderParameters parameters;
	
	private Connection con;	
	private Scanner scanner;
	
	private Logger logger;
	
	
	public SerialLoader(String name, IParameters parameters, int parallelOrder) {
		super();
		
		this.name=name;
		this.parallelOrder=parallelOrder;
		this.parameters=(LoaderParameters)parameters;
		logger=new Logger(name);				
	}
	
	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void SetName(String name) {
		this.name=name;
		
	}

	@Override
	public IParameters getParameters() {
		return this.parameters;
	}

	@Override
	public void setParameters(IParameters parameters) {
		this.parameters=(LoaderParameters)parameters;
		
	}
	
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
	
	public void truncateTable() {
		Connection cont=null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			cont = DriverManager.getConnection(this.parameters.getConnection().getConnectionString(),
					this.parameters.getConnection().getUser(), this.parameters.getConnection().getPassword());
			Statement stmt = cont.createStatement();
			stmt.executeUpdate("truncate table " + this.parameters.getTableName());
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
	public void load() {
		
		try {
		
			logger.start();					
			
			String v_owner;	
			if(this.parameters.getTableName().indexOf(".")<0) {
				v_owner=this.parameters.getConnection().getUser().toUpperCase();
			}
			else {
				v_owner=this.parameters.getTableName().substring(0, this.parameters.getTableName().indexOf(".")).toUpperCase();
			}		
			String v_table=this.parameters.getTableName().substring(this.parameters.getTableName().indexOf(".")+1).toUpperCase();
			
			
			String metadataQuery=
			"select count(*) cnt\r\n" + 
			"from all_tab_columns\r\n" + 
			"where owner='"+v_owner+"'\r\n" + 
			"and table_name='"+v_table+"'\r\n" + 
			"order by column_id";
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con=DriverManager.getConnection(this.parameters.getConnection().getConnectionString(),this.parameters.getConnection().getUser(),this.parameters.getConnection().getPassword());
			if(this.parameters.isDirectPathInsert()) {
				con.setAutoCommit(true);
			}
			else {
				con.setAutoCommit(false);
			}
			Statement stmt=con.createStatement();
			ResultSet rs=stmt.executeQuery(metadataQuery);
			rs.next();
			
			String parameters="?,".repeat(rs.getInt(1));
			parameters=parameters.substring(0,parameters.length()-1);
			
			String append="";
			if(this.parameters.isDirectPathInsert()) append="/*+ append_values */ ";
			String insertQueryString="insert " + append + "into " + this.parameters.getTableName() + " values (" + parameters + ")";
			PreparedStatement insertQuery=con.prepareStatement(insertQueryString);
			
			
			String nlsLang="alter session set NLS_LANGUAGE='TURKISH'";
			String nlsNumeric="alter session set NLS_NUMERIC_CHARACTERS='"+this.parameters.getDecimalSeperator()+" '";
			String nlsDate="alter session set NLS_DATE_FORMAT='"+this.parameters.getDateTimeFormat()+"'";		
			String nlsTimestamp="alter session set NLS_TIMESTAMP_FORMAT='"+this.parameters.getTimestampFormat()+"'";
	
			
			stmt.executeUpdate(nlsLang);
			stmt.executeUpdate(nlsNumeric);
			stmt.executeUpdate(nlsDate);
			stmt.executeUpdate(nlsTimestamp);
			
			if(this.parameters.isTruncateTargetTable() && this.parameters.getParallelCount()==1) {
				truncateTable();
			}
			
			//load
			String fileName=this.parameters.getFile();
			if(this.parameters.getParallelCount()>1) {
				fileName = fileName.substring(0, fileName.lastIndexOf("."))  + "_" + parallelOrder + fileName.substring(fileName.lastIndexOf("."));
			}
			scanner = new Scanner(new File(fileName),"windows-1254");
			scanner.useDelimiter(this.parameters.getRowDelimiter());
			
			String rowStr;
			String[] columns;
			int currentBatchSize=0;			
			long readedRowCount=0;
			
			logger.message("Initialized");
			
			while(scanner.hasNext()) {
				rowStr=scanner.next();
				readedRowCount++;
				if(this.parameters.getSkipRowCount()<readedRowCount) {
					columns=rowStr.split(this.parameters.getColumnDelimiter());
					for(int i=0;i<columns.length;i++) {
						insertQuery.setString(i+1, columns[i]);
					}
					insertQuery.addBatch();
					currentBatchSize++;
					if(currentBatchSize==this.parameters.getBatchSize()) {
						insertQuery.executeBatch();
						if(!this.parameters.isDirectPathInsert() && !this.parameters.isCommitAfterLoad()) {
							con.commit();
						}
											
						logger.step(currentBatchSize, "rows inserted");
												
						currentBatchSize=0;
					}
				}
			}
			if(currentBatchSize>0) {
				insertQuery.executeBatch();
				if(!this.parameters.isDirectPathInsert() && !this.parameters.isCommitAfterLoad()) {
					con.commit();
				}
				logger.step(currentBatchSize, "rows inserted");
			}
			
			if(this.parameters.isCommitAfterLoad()) {
				con.commit();
			}
			con.close();
			scanner.close();
			
			
			logger.end();
			
		}
		catch(Exception e) {
			try {
				con.close();
				scanner.close();
				
			}
			catch(Exception e1) {}
			
			logger.error();
			errorListener.onError(this.name,e);	
		}
		
	}

	@Override
	public void run() {
		load();		
	}

	

}
