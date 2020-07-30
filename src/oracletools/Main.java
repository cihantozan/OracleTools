package oracletools;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import oracletools.loader.Loader;
import oracletools.loader.LoaderParameters;
import oracletools.loader.SerialLoader;
import oracletools.transfer.SerialTransfer;
import oracletools.transfer.Transfer;
import oracletools.transfer.TransferParameters;
import oracletools.unloader.SerialUnloader;
import oracletools.unloader.Unloader;
import oracletools.unloader.UnloaderParameters;
import oracletools.util.FileCombiner;
import oracletools.util.OracleConnection;

//connectionlar �ifreli bi�imde kaydedilecek
//TNS varsa al�nacak
//parametrelerin default de�erleri???
//unload,load excel
//jdk n�n �nceki versiyonlar�nda da dene
//parametrelerin config dosyas�ndan al�nmas�
//unloaderda t�rk�e karakter problemi var.

public class Main {

	public static void main(String[] args) throws Exception {
		
		
		//UNLOADER	
		
		
		OracleConnection connection=new OracleConnection("USER1", "Tvn279um", "localhost", 1521, "ORCL");
		String file="C:\\Users\\Cihan\\Documents\\export.txt";
		String query="select * from user1.table1 where rownum<=1000";
		String columnDelimiter=";"; 
		String rowDelimiter="\r\n";
		boolean addColumnNames=true;
		String dateFormat="dd.MM.yyyy";
		String dateTimeFormat="dd.MM.yyyy HH:mm:ss";
		char decimalSeperator='.';
		int fetchSize=1000000;
		int rowCountMessageLength=1000000;
		int parallelCount=4;
		String[] parallelDivisorColumns= {"A"};
		boolean combineFiles=true;
		
		UnloaderParameters unloaderParameters=new UnloaderParameters(connection, file, query, columnDelimiter, rowDelimiter, addColumnNames, dateFormat, dateTimeFormat, decimalSeperator, fetchSize, rowCountMessageLength, parallelCount, parallelDivisorColumns, combineFiles); 
		
		
		Unloader unloader=new Unloader(unloaderParameters);
		unloader.unload();
		
		
		/*
		Unloader unloader=new Unloader();
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
		boolean commitAfterLoad=false;
		int parallelCount=1;
		
		LoaderParameters loaderParameters = new LoaderParameters(connection, file, tableName, columnDelimiter, rowDelimiter, skipRowCount, dateTimeFormat, timestampFormat, decimalSeperator, batchSize, truncateTargetTable, directPathInsert, commitAfterLoad, parallelCount); 
		
		Loader loader = new Loader(loaderParameters);
		loader.load();
		*/
		
		//TRANSFER
		/*
		OracleConnection sourceConnection=new OracleConnection("USER1", "Tvn279um", "localhost", 1521, "ORCL");
		OracleConnection targetConnection=new OracleConnection("USER1", "Tvn279um", "localhost", 1521, "ORCL");
		String sourceQuery="select * from user1.table1 where rownum<=1000";
		String targetTable="user1.table2";
		int batchSize=500000;	
		boolean truncateTargetTable=true;
		boolean directPathInsert=true;
		boolean commitAfterLoad=false;
		int parallelCount=2;
		String[] parallelDivisorColumns= {"A"};
		
		TransferParameters transferParameters=new TransferParameters(sourceConnection, targetConnection, sourceQuery, targetTable, batchSize, truncateTargetTable, directPathInsert, commitAfterLoad, parallelCount, parallelDivisorColumns);
		Transfer transfer=new Transfer(transferParameters);
		transfer.transfer();
		*/
		
		
		/*
		String[] files = {"C:\\Users\\Cihan\\Documents\\a_1.txt","C:\\Users\\Cihan\\Documents\\a_2.txt","C:\\Users\\Cihan\\Documents\\a_3.txt","C:\\Users\\Cihan\\Documents\\a_4.txt"};
		String targetFileName="a.txt";
		String rowDelimiter="\r\n";
		FileCombiner combiner=new FileCombiner(files, targetFileName, rowDelimiter);
		combiner.combine();
		*/
		
	}

}
