package oracletools.unloader;

import java.io.FileOutputStream;
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

import oracletools.util.IOracleTool;
import oracletools.util.IParameters;
import oracletools.util.Logger;
import oracletools.util.listeners.ErrorListener;
import oracletools.util.listeners.LoggerActivityListener;




public class SerialUnloader implements IOracleTool {
		
	private String name;
	@Override
	public String getName() {
		return this.name;
	}
	@Override
	public void SetName(String name) {
		this.name=name;		
	}

	
	private UnloaderParameters parameters;
		
	@Override
	public IParameters getParameters() {		
		return this.parameters;
	}
	@Override
	public void setParameters(IParameters parameters) {
		this.parameters=(UnloaderParameters) parameters;
	}
	
	private int parallelOrder;
		
	public int getParallelOrder() {
		return parallelOrder;
	}
	public void setParallelOrder(int parallelOrder) {
		this.parallelOrder = parallelOrder;
	}


	private Connection con;
	private FileOutputStream stream;
	private OutputStreamWriter writer;
	
	private SimpleDateFormat simpleTimeFormat;
	private SimpleDateFormat simpleDateFormat;
	private SimpleDateFormat simpleDateTimeFormat;
	private SimpleDateFormat simpleTimeStampFormat;
	private DecimalFormat decimalFormat;
	
	
	
	private Logger logger;
		
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
	
	
	

	
	
	public SerialUnloader(String name,IParameters parameters, int parallelOrder) {
		super();
		this.name=name;
		this.parameters = (UnloaderParameters) parameters;
		this.parallelOrder=parallelOrder;
		
		//Date format
		simpleTimeFormat=new SimpleDateFormat("HH:mm:ss");
		simpleDateFormat=new SimpleDateFormat(this.parameters.getDateFormat());
		simpleDateTimeFormat=new SimpleDateFormat(this.parameters.getDateTimeFormat());
		simpleTimeStampFormat=new SimpleDateFormat(this.parameters.getDateTimeFormat()+".S");
		
		//Decimal format
		DecimalFormatSymbols symbols=new DecimalFormatSymbols();		
		symbols.setDecimalSeparator(this.parameters.getDecimalSeperator());		
		decimalFormat=new DecimalFormat("",symbols);
		decimalFormat.setGroupingUsed(false);
		
		
		logger=new Logger(name);
		
		
	}



	private void unload() {		

		try {
						
			logger.start();
			
			String fileName=this.parameters.getFile();
			if(this.parameters.getParallelCount()>1) {
				fileName = fileName.substring(0, fileName.lastIndexOf("."))  + "_" + parallelOrder + fileName.substring(fileName.lastIndexOf("."));
			}
			stream=new FileOutputStream(fileName);
			writer=new OutputStreamWriter(stream,"windows-1254");
			StringBuilder row=new StringBuilder();
			
			
			//run query
			
			String query=this.parameters.getQuery();
			String columns="''||";
			for(int i=0; i<this.parameters.getParallelDivisorColumns().length; i++) {
				columns+=this.parameters.getParallelDivisorColumns()[i];
				if(i != this.parameters.getParallelDivisorColumns().length-1) {
					columns+="||";
				}
			}
			
			if (this.parameters.getParallelCount()>1) {
				query = "select * from ( " + query + " ) where ora_hash(" + columns + "," + (this.parameters.getParallelCount() - 1) + ")=" + (this.parallelOrder - 1);
			}
			
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con=DriverManager.getConnection(this.parameters.getConnection().getConnectionString(),this.parameters.getConnection().getUser(),this.parameters.getConnection().getPassword());
			Statement stmt=con.createStatement();
			stmt.setFetchSize(this.parameters.getFetchSize());			
			ResultSet rs=stmt.executeQuery(query);
			rs.setFetchSize(this.parameters.getFetchSize());
			ResultSetMetaData resultSetMetaData = rs.getMetaData();									
			boolean firstRow=rs.next();
						
			logger.message("Query executed");
			
			//columnNames
			if(
				  this.parameters.isAddColumnNames() 
				  && firstRow 
				  && (
							 (parallelOrder<=1 && this.parameters.isCombineFiles())
					      || (!this.parameters.isCombineFiles())
					 )
			){
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
				if(rowCount == this.parameters.getRowCountMessageLength()) {
					logger.step(rowCount, "rows");
					rowCount=0;
				}
				
				hasRows=rowInfo.getHasRowsAfter();
			}
			
			logger.step(rowCount, "rows");		
									
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
						
			logger.error();
			errorListener.onError(this.name,e);	
		}
	}
	
	private String getColumnNames(ResultSetMetaData resultSetMetaData) throws SQLException {
		StringBuilder sb=new StringBuilder();
		for(int i=1; i<=resultSetMetaData.getColumnCount(); i++) {
			sb.append(resultSetMetaData.getColumnName(i));
			if(i!=resultSetMetaData.getColumnCount()) {
				sb.append(this.parameters.getColumnDelimiter());
			}
			
		}
		sb.append(this.parameters.getRowDelimiter());
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
				row.append(this.parameters.getColumnDelimiter());
			}
		}
		hasRowsAfter=rs.next();
		if(hasRowsAfter) {
			row.append(this.parameters.getRowDelimiter());
		}
	
		return new RowInfo(row.toString(), hasRowsAfter);
	}



	@Override
	public void run() {
		
		unload();
		
	}
	
	
	
	
}


