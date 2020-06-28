package oracletools;

import java.io.IOException;
import java.sql.SQLException;

import oracletools.filecombiner.FileCombiner;
import oracletools.loader.Loader;
import oracletools.transfer.Transfer;
import oracletools.unloader.Unloader;

//connectionlar �ifreli bi�imde kaydedilecek
//TNS varsa al�nacak
//parametrelerin default de�erleri???
//loglara rows per second ekle, avg rps de ekle, formatla
//trucate i�in ayr� log
//loglama utile gitsin
//classlara run koy
//directpath gibi de�i�iklikler loadera da yap�lacak
//paralellik
//parallel unload mult.files, paralllel unload one file, parallel loaf from mult. files, parallel transfer
//file combiner loglamas� yap�lacak

public class Main {

	public static void main(String[] args) throws Exception {
		
		//UNLOADER	
		/*
		OracleConnection connection=new OracleConnection("USER1", "Tvn279um", "localhost", 1521, "ORCL");
		String file="C:\\Users\\Cihan\\Documents\\export.txt";
		String query="select * from user1.table1";
		String columnDelimiter=";"; 
		String rowDelimiter="\r\n";
		boolean addColumnNames=true;
		String dateFormat="dd.MM.yyyy";
		String dateTimeFormat="dd.MM.yyyy HH:mm:ss";
		char decimalSeperator='.';
		int fetchSize=1000000;
		int rowCountMessageLength=1000000;
		
		Unloader unloader=new Unloader(connection,file,query,columnDelimiter, rowDelimiter, addColumnNames, dateFormat, dateTimeFormat, decimalSeperator,fetchSize, rowCountMessageLength);
		unloader.unload();
		*/
		
		
		//LOADER
		/*
		OracleConnection connection=new OracleConnection("USER1", "Tvn279um", "localhost", 1521, "ORCL");
		String file="C:\\Users\\Cihan\\Documents\\export.txt"; 
		String tableName="user1.table2";
		String columnDelimiter=";";
		String rowDelimiter="\r\n"; 
		int skipRowCount=1;
		String dateTimeFormat="dd.mm.yyyy hh24:mi:ss"; 
		String timestampFormat="dd.mm.yyyy hh24:mi:ss.ff";
		char decimalSeperator='.';
		int batchSize=1000000;
		boolean truncateTargetTable=true;
		boolean directPathInsert=false;
		boolean commitAfterLoad=true;
		
		Loader loader = new Loader(connection, file, tableName, columnDelimiter, rowDelimiter, skipRowCount, dateTimeFormat, timestampFormat, decimalSeperator, batchSize, truncateTargetTable, directPathInsert, commitAfterLoad);
		loader.load();
		*/
		
		//TRANSFER
		
		OracleConnection sourceConnection=new OracleConnection("USER1", "Tvn279um", "localhost", 1521, "ORCL");
		OracleConnection targetConnection=new OracleConnection("USER1", "Tvn279um", "localhost", 1521, "ORCL");
		String sourceQuery="select * from user1.table1 where rownum<=102";
		String targetTable="user1.table2";
		int batchSize=500000;	
		boolean truncateTargetTable=true;
		boolean directPathInsert=false;
		boolean commitAfterLoad=false;
		
		Transfer transfer=new Transfer(sourceConnection, targetConnection, sourceQuery, targetTable, batchSize, truncateTargetTable, directPathInsert, commitAfterLoad);
		transfer.transfer();
		
		
		/*
		String[] files = {"C:\\Users\\Cihan\\Documents\\a_1.txt","C:\\Users\\Cihan\\Documents\\a_2.txt","C:\\Users\\Cihan\\Documents\\a_3.txt","C:\\Users\\Cihan\\Documents\\a_4.txt"};
		String targetFileName="a.txt";		
		FileCombiner combiner=new FileCombiner(files, targetFileName);
		combiner.combine();
		*/
		
	}

}
