package oracletools.loader;

import oracletools.util.IParameters;
import oracletools.util.OracleConnection;

public class LoaderParameters implements IParameters{
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
	private int parallelCount;	
	
	
	public LoaderParameters() {	
	}
	
	public LoaderParameters(OracleConnection connection, String file, String tableName, String columnDelimiter,
			String rowDelimiter, int skipRowCount, String dateTimeFormat, String timestampFormat, char decimalSeperator,
			int batchSize, boolean truncateTargetTable, boolean directPathInsert, boolean commitAfterLoad,
			int parallelCount) {
		super();
		this.connection = connection;
		this.file = file;
		this.tableName = tableName;
		this.columnDelimiter = columnDelimiter;
		this.rowDelimiter = rowDelimiter;
		this.skipRowCount = skipRowCount;
		this.dateTimeFormat = dateTimeFormat;
		this.timestampFormat = timestampFormat;
		this.decimalSeperator = decimalSeperator;
		this.batchSize = batchSize;
		this.truncateTargetTable = truncateTargetTable;
		this.directPathInsert = directPathInsert;
		this.commitAfterLoad = commitAfterLoad;
		this.parallelCount = parallelCount;
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
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
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
	public int getSkipRowCount() {
		return skipRowCount;
	}
	public void setSkipRowCount(int skipRowCount) {
		this.skipRowCount = skipRowCount;
	}
	public String getDateTimeFormat() {
		return dateTimeFormat;
	}
	public void setDateTimeFormat(String dateTimeFormat) {
		this.dateTimeFormat = dateTimeFormat;
	}
	public String getTimestampFormat() {
		return timestampFormat;
	}
	public void setTimestampFormat(String timestampFormat) {
		this.timestampFormat = timestampFormat;
	}
	public char getDecimalSeperator() {
		return decimalSeperator;
	}
	public void setDecimalSeperator(char decimalSeperator) {
		this.decimalSeperator = decimalSeperator;
	}
	public int getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	public boolean isTruncateTargetTable() {
		return truncateTargetTable;
	}
	public void setTruncateTargetTable(boolean truncateTargetTable) {
		this.truncateTargetTable = truncateTargetTable;
	}
	public boolean isDirectPathInsert() {
		return directPathInsert;
	}
	public void setDirectPathInsert(boolean directPathInsert) {
		this.directPathInsert = directPathInsert;
	}
	public boolean isCommitAfterLoad() {
		return commitAfterLoad;
	}
	public void setCommitAfterLoad(boolean commitAfterLoad) {
		this.commitAfterLoad = commitAfterLoad;
	}
	public int getParallelCount() {
		return parallelCount;
	}
	public void setParallelCount(int parallelCount) {
		this.parallelCount = parallelCount;
	}
	
}
