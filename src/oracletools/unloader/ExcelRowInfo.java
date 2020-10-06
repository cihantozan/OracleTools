package oracletools.unloader;

import org.apache.poi.ss.usermodel.Row;

public class ExcelRowInfo{
	
	private Row excelRow;
	
	private boolean hasRowsAfter;
	
	public ExcelRowInfo() {		
	}
	public ExcelRowInfo(Row excelRow, boolean hasRowsAfter) {
		super();
		this.excelRow = excelRow;
		this.hasRowsAfter = hasRowsAfter;
	}
	public Row getExcelRow() {
		return excelRow;
	}
	public void setExcelRow(Row excelRow) {
		this.excelRow = excelRow;
	}
	public boolean getHasRowsAfter() {
		return hasRowsAfter;
	}
	public void setHasRowsAfter(boolean hasRowsAfter) {
		this.hasRowsAfter = hasRowsAfter;
	}
	
	
	
}
