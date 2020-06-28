package oracletools.loader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import oracletools.util.Logger;
import oracletools.util.OracleConnection;

public class Loader {


	private OracleConnection connection;
	private String file;
	private String tableName; 
	private String columnDelimiter; 
	private String rowDelimiter;
	private int skipRowCount; 
	private String dateTimeFormat;
	private String timestampFormat;
	private char decimalSeperator; 
	private int batchSize;	
	private boolean truncateTargetTable;
	private boolean directPathInsert;
	private boolean commitAfterLoad;
	
	private Connection con;	
	private Scanner scanner;
	
	private Logger logger;
	
	public Loader(OracleConnection connection, String file, String tableName, String columnDelimiter, String rowDelimiter, int skipRowCount, String dateTimeFormat, String timestampFormat, char decimalSeperator, int batchSize, boolean truncateTargetTable, boolean directPathInsert, boolean commitAfterLoad) {
		super();
		this.connection = connection;
		this.file = file;
		this.tableName = tableName;
		this.columnDelimiter = columnDelimiter;
		this.rowDelimiter = rowDelimiter;
		this.skipRowCount = skipRowCount;		
		this.dateTimeFormat = dateTimeFormat;
		this.timestampFormat=timestampFormat;
		this.decimalSeperator = decimalSeperator;
		this.batchSize = batchSize;
		this.truncateTargetTable=truncateTargetTable;
		this.directPathInsert=directPathInsert;
		this.commitAfterLoad=commitAfterLoad;
		
		logger=new Logger();
		
	}
	
	public void load() throws FileNotFoundException, UnsupportedEncodingException, ClassNotFoundException, SQLException {
		
		try {
		
			logger.start();
		
			
			String v_owner;	
			if(tableName.indexOf(".")<0) {
				v_owner=connection.getUser().toUpperCase();
			}
			else {
				v_owner=tableName.substring(0, tableName.indexOf(".")).toUpperCase();
			}		
			String v_table=tableName.substring(tableName.indexOf(".")+1).toUpperCase();
			
			
			String metadataQuery=
			"select count(*) cnt\r\n" + 
			"from all_tab_columns\r\n" + 
			"where owner='"+v_owner+"'\r\n" + 
			"and table_name='"+v_table+"'\r\n" + 
			"order by column_id";
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con=DriverManager.getConnection(connection.getConnectionString(),connection.getUser(),connection.getPassword());
			if(directPathInsert) {
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
			if(directPathInsert) append="/*+ append_values */ ";
			String insertQueryString="insert " + append + "into " + tableName + " values (" + parameters + ")";
			PreparedStatement insertQuery=con.prepareStatement(insertQueryString);
			
			
			String nlsLang="alter session set NLS_LANGUAGE='TURKISH'";
			String nlsNumeric="alter session set NLS_NUMERIC_CHARACTERS='"+decimalSeperator+" '";
			String nlsDate="alter session set NLS_DATE_FORMAT='"+dateTimeFormat+"'";		
			String nlsTimestamp="alter session set NLS_TIMESTAMP_FORMAT='"+timestampFormat+"'";
	
			
			stmt.executeUpdate(nlsLang);
			stmt.executeUpdate(nlsNumeric);
			stmt.executeUpdate(nlsDate);
			stmt.executeUpdate(nlsTimestamp);
			
			if(truncateTargetTable) {
				stmt.executeUpdate("truncate table "+tableName);
				logger.message("Truncated");
			}
			
			//load
			scanner = new Scanner(new File(file),"windows-1254");
			scanner.useDelimiter(rowDelimiter);
			
			String rowStr;
			String[] columns;
			int currentBatchSize=0;			
			long readedRowCount=0;
			
			logger.message("Initialized");
			
			while(scanner.hasNext()) {
				rowStr=scanner.next();
				readedRowCount++;
				if(skipRowCount<readedRowCount) {
					columns=rowStr.split(columnDelimiter);
					for(int i=0;i<columns.length;i++) {
						insertQuery.setString(i+1, columns[i]);
					}
					insertQuery.addBatch();
					currentBatchSize++;
					if(currentBatchSize==batchSize) {
						insertQuery.executeBatch();
						if(!directPathInsert && !commitAfterLoad) {
							con.commit();
						}
											
						logger.step(currentBatchSize, "rows inserted");
												
						currentBatchSize=0;
					}
				}
			}
			if(currentBatchSize>0) {
				insertQuery.executeBatch();
				if(!directPathInsert && !commitAfterLoad) {
					con.commit();
				}
				logger.step(currentBatchSize, "rows inserted");
			}
			
			if(commitAfterLoad) {
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
			
			throw e;
		}
		
	}

}
