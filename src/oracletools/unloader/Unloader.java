package oracletools.unloader;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import oracletools.util.Logger;
import oracletools.util.OracleConnection;




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
	
	private Connection con;
	private FileOutputStream stream;
	private OutputStreamWriter writer;
	
	private SimpleDateFormat simpleTimeFormat;
	private SimpleDateFormat simpleDateFormat;
	private SimpleDateFormat simpleDateTimeFormat;
	private SimpleDateFormat simpleTimeStampFormat;
	private DecimalFormat decimalFormat;

	
	private Logger logger;
	
	
	public Unloader(OracleConnection connection, String file, String query, String columnDelimiter, String rowDelimiter,boolean addColumnNames, String dateFormat, String dateTimeFormat, char decimalSeperator, int fetchSize, int rowCountMessageLength) {
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
		
		//Date format
		simpleTimeFormat=new SimpleDateFormat("HH:mm:ss");
		simpleDateFormat=new SimpleDateFormat(this.dateFormat);
		simpleDateTimeFormat=new SimpleDateFormat(this.dateTimeFormat);
		simpleTimeStampFormat=new SimpleDateFormat(this.dateTimeFormat+".S");
		
		//Decimal format
		DecimalFormatSymbols symbols=new DecimalFormatSymbols();		
		symbols.setDecimalSeparator(this.decimalSeperator);		
		decimalFormat=new DecimalFormat("",symbols);
		decimalFormat.setGroupingUsed(false);
		
		logger=new Logger();
	}



	public void unload() throws ClassNotFoundException, SQLException, IOException {		

		try {
						
			logger.start();
		
			stream=new FileOutputStream(file);
			writer=new OutputStreamWriter(stream,"windows-1254");
			StringBuilder row=new StringBuilder();
			
			
			//run query
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con=DriverManager.getConnection(connection.getConnectionString(),connection.getUser(),connection.getPassword());
			Statement stmt=con.createStatement();
			stmt.setFetchSize(fetchSize);			
			ResultSet rs=stmt.executeQuery(query);
			rs.setFetchSize(fetchSize);
			ResultSetMetaData resultSetMetaData = rs.getMetaData();									
			boolean firstRow=rs.next();
						
			logger.message("Query executed");
			
			//columnNames
			if(addColumnNames && firstRow) {
				String rowColumnNames=getColumnNames(resultSetMetaData);
				writer.write(rowColumnNames);		
			}						
						
			//unload			
			int rowCount=0;
			boolean hasRows=firstRow;
			while(hasRows) {				
				RowInfo rowInfo=getRow(rs, resultSetMetaData, row);				
				writer.write(rowInfo.getRowString());
				rowCount++;
				
				//rowCountMessaging
				if(rowCount == rowCountMessageLength) {
					logger.step(rowCount, "rows extracted");
					rowCount=0;
				}
				
				hasRows=rowInfo.getHasRowsAfter();
			}
			
			logger.step(rowCount, "rows extracted");		
									
			writer.close();
			stream.close();
			con.close();
			
			logger.end();						
		
		}
		catch(Exception e) {
			try {
				writer.close();
				stream.close();
				con.close();
			} 
			catch (Exception e1) {}
			
			throw e;
		}
	}
	
	private String getColumnNames(ResultSetMetaData resultSetMetaData) throws SQLException {
		StringBuilder sb=new StringBuilder();
		for(int i=1; i<=resultSetMetaData.getColumnCount(); i++) {
			sb.append(resultSetMetaData.getColumnName(i));
			if(i!=resultSetMetaData.getColumnCount()) {
				sb.append(columnDelimiter);
			}
			
		}
		sb.append(rowDelimiter);
		return sb.toString();
	}
	
	private RowInfo getRow(ResultSet rs,ResultSetMetaData resultSetMetaData,StringBuilder row) throws SQLException {
		row.setLength(0);
				
		boolean hasRowsAfter=false;
		
		for(int i=1;i<=resultSetMetaData.getColumnCount();i++) {
			Object column=rs.getObject(i);
			String columnText;
			if(column!=null) {
				if(resultSetMetaData.getColumnTypeName(i).equals("DATE")) {
					Date columnDate=rs.getDate(i);														
					if (simpleTimeFormat.format(columnDate).equals("00:00:00")) {								
						columnText=simpleDateFormat.format(columnDate);
					}
					else {								
						columnText=simpleDateTimeFormat.format(columnDate);
					}
				}
				else if(resultSetMetaData.getColumnTypeName(i).equals("TIMESTAMP")) {
					Timestamp columnTimeStamp = rs.getTimestamp(i);
					columnText=simpleTimeStampFormat.format(columnTimeStamp);
				}
				else if(resultSetMetaData.getColumnTypeName(i).equals("NUMBER")){
					BigDecimal columnDecimal=rs.getBigDecimal(i);
					columnText=decimalFormat.format(columnDecimal);
				}
				else {
					columnText=column.toString();
				}
				row.append(columnText);						
			}
			if(i!=resultSetMetaData.getColumnCount()) {
				row.append(columnDelimiter);
			}
		}
		hasRowsAfter=rs.next();
		if(hasRowsAfter) {
			row.append(rowDelimiter);
		}
	
		return new RowInfo(row.toString(), hasRowsAfter);
	}
	
	
}


