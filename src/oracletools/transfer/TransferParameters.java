package oracletools.transfer;

import oracletools.util.IParameters;
import oracletools.util.OracleConnection;

public class TransferParameters implements IParameters {

	private OracleConnection sourceConnection;
	private OracleConnection targetConnection;
	private String sourceQuery;
	private String targetTable;
	private int batchSize;	
	private boolean truncateTargetTable;
	private boolean directPathInsert;
	private boolean commitAfterLoad;
	private int parallelCount;
	private String[] parallelDivisorColumns;
	
	public TransferParameters() {	
	}
	
	public TransferParameters(OracleConnection sourceConnection, OracleConnection targetConnection, String sourceQuery,
			String targetTable, int batchSize, boolean truncateTargetTable, boolean directPathInsert,
			boolean commitAfterLoad, int parallelCount, String[] parallelDivisorColumns) {
		super();
		this.sourceConnection = sourceConnection;
		this.targetConnection = targetConnection;
		this.sourceQuery = sourceQuery;
		this.targetTable = targetTable;
		this.batchSize = batchSize;
		this.truncateTargetTable = truncateTargetTable;
		this.directPathInsert = directPathInsert;
		this.commitAfterLoad = commitAfterLoad;
		this.parallelCount = parallelCount;
		this.parallelDivisorColumns = parallelDivisorColumns;
	}
	
	
	public OracleConnection getSourceConnection() {
		return sourceConnection;
	}
	public void setSourceConnection(OracleConnection sourceConnection) {
		this.sourceConnection = sourceConnection;
	}
	public OracleConnection getTargetConnection() {
		return targetConnection;
	}
	public void setTargetConnection(OracleConnection targetConnection) {
		this.targetConnection = targetConnection;
	}
	public String getSourceQuery() {
		return sourceQuery;
	}
	public void setSourceQuery(String sourceQuery) {
		this.sourceQuery = sourceQuery;
	}
	public String getTargetTable() {
		return targetTable;
	}
	public void setTargetTable(String targetTable) {
		this.targetTable = targetTable;
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
	public String[] getParallelDivisorColumns() {
		return parallelDivisorColumns;
	}
	public void setParallelDivisorColumns(String[] parallelDivisorColumns) {
		this.parallelDivisorColumns = parallelDivisorColumns;
	}
	
	
		
	
}
