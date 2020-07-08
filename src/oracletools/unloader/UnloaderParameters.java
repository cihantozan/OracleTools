package oracletools.unloader;

import oracletools.util.IParameters;
import oracletools.util.OracleConnection;

public class UnloaderParameters implements IParameters {
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
	private boolean combineFiles;
		
	public OracleConnection getConnection() {
		return connection;
	}
	public void setConnection(OracleConnection connection) {
		this.connection = connection;
	}
	public String getFile() {
		return file;
	}
	public void setFile(String file) {
		this.file = file;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	public String getColumnDelimiter() {
		return columnDelimiter;
	}
	public void setColumnDelimiter(String columnDelimiter) {
		this.columnDelimiter = columnDelimiter;
	}
	public String getRowDelimiter() {
		return rowDelimiter;
	}
	public void setRowDelimiter(String rowDelimiter) {
		this.rowDelimiter = rowDelimiter;
	}
	public boolean isAddColumnNames() {
		return addColumnNames;
	}
	public void setAddColumnNames(boolean addColumnNames) {
		this.addColumnNames = addColumnNames;
	}
	public String getDateFormat() {
		return dateFormat;
	}
	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}
	public String getDateTimeFormat() {
		return dateTimeFormat;
	}
	public void setDateTimeFormat(String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}
	public char getDecimalSeperator() {
		return decimalSeperator;
	}
	public void setDecimalSeperator(char decimalSeperator) {
		this.decimalSeperator = decimalSeperator;
	}
	public int getFetchSize() {
		return fetchSize;
	}
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}
	public int getRowCountMessageLength() {
		return rowCountMessageLength;
	}
	public void setRowCountMessageLength(int rowCountMessageLength) {
		this.rowCountMessageLength = rowCountMessageLength;
	}
	public int getParallelCount() {
		return parallelCount;
	}
	public void setParallelCount(int parallelCount) {
		this.parallelCount = parallelCount;
	}
	public String[] getParallelDivisorColumns() {
		return parallelDivisorColumns;
	}
	public void setParallelDivisorColumns(String[] parallelDivisorColumns) {
		this.parallelDivisorColumns = parallelDivisorColumns;
	}
	public boolean isCombineFiles() {
		return combineFiles;
	}
	public void setCombineFiles(boolean combineFiles) {
		this.combineFiles = combineFiles;
	}	
}
