package oracletools.unloader;

public class RowInfo{
	
	private String rowString;
	private boolean hasRowsAfter;
	
	public RowInfo() {		
	}
	public RowInfo(String rowString, boolean hasRowsAfter) {
		super();
		this.rowString = rowString;
		this.hasRowsAfter = hasRowsAfter;
	}
	public String getRowString() {
		return rowString;
	}
	public void setRowString(String rowString) {
		this.rowString = rowString;
	}
	public boolean getHasRowsAfter() {
		return hasRowsAfter;
	}
	public void setHasRowsAfter(boolean hasRowsAfter) {
		this.hasRowsAfter = hasRowsAfter;
	}
	
	
	
}
