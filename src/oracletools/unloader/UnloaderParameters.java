package oracletools.unloader;

import java.util.Arrays;

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
	
	
		
	public UnloaderParameters() {		
	}
	
	public UnloaderParameters(OracleConnection connection, String file, String query, String columnDelimiter,
			String rowDelimiter, boolean addColumnNames, String dateFormat, String dateTimeFormat,
			char decimalSeperator, int fetchSize, int rowCountMessageLength, int parallelCount,
			String[] parallelDivisorColumns, boolean combineFiles) {
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
		this.rowCountMessageLength = rowCountMessageLength;
		this.parallelCount = parallelCount;
		this.parallelDivisorColumns = parallelDivisorColumns;
		this.combineFiles = combineFiles;
	}
		
	
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

	@Override
	public String toString() {
		return "UnloaderParameters [connection=" + connection + ", file=" + file + ", query=" + query
				+ ", columnDelimiter=" + columnDelimiter + ", rowDelimiter=" + rowDelimiter + ", addColumnNames="
				+ addColumnNames + ", dateFormat=" + dateFormat + ", dateTimeFormat=" + dateTimeFormat
				+ ", decimalSeperator=" + decimalSeperator + ", fetchSize=" + fetchSize + ", rowCountMessageLength="
				+ rowCountMessageLength + ", parallelCount=" + parallelCount + ", parallelDivisorColumns="
				+ Arrays.toString(parallelDivisorColumns) + ", combineFiles=" + combineFiles + "]";
	}
	
	
}
