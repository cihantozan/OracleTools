package oracletools.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;

import oracletools.util.listeners.LoggerActivityListener;

public class FileCombiner {
	
	private String[] files;
	private String targetFileName;
	private String rowDelimiter;
	
	private Logger logger;
	
	public FileCombiner(String[] files, String targetFileName, String rowDelimiter) {
		super();
		this.files = files;
		this.targetFileName = targetFileName;
		this.rowDelimiter=rowDelimiter;
		
		logger=new Logger("FileCombiner");
	}
	
	public void combine() throws Exception {
		
		logger.start();
		
		if(rowDelimiter!=null && !rowDelimiter.equals("")) {
			FileOutputStream currStream=new FileOutputStream(files[0],true);
			OutputStreamWriter currWriter=new OutputStreamWriter(currStream,"windows-1254");
			currWriter.write(rowDelimiter);
			currWriter.close();
			currStream.close();
		}
		
		for(int i=1;i<files.length;i++) {
			
			if(rowDelimiter!=null && !rowDelimiter.equals("") && i<files.length-1) {
				FileOutputStream currStream=new FileOutputStream(files[i],true);
				OutputStreamWriter currWriter=new OutputStreamWriter(currStream,"windows-1254");
				currWriter.write(rowDelimiter);
				currWriter.close();
				currStream.close();
			}
			
			FileInputStream fromStream=new FileInputStream(files[i]);
			FileOutputStream toStream=new FileOutputStream(files[0],true);
			FileChannel c1 = fromStream.getChannel();
		    FileChannel c2 = toStream.getChannel();
		    c2.transferFrom(c1, c2.size(), c1.size());
		    c1.close();
		    c2.close();
		    fromStream.close();
		    toStream.close();
		    
		    File f=new File(files[i]);
		    boolean r=f.delete();
		    if(!r) throw new Exception("File delete error");
		    
		    logger.step(1, "files merged");
		}
		File target=new File(files[0]);
		//File targetNew=new File(target.getParent()+"\\\\"+targetFileName);
		File targetNew=new File(targetFileName);
		targetNew.delete();
		boolean r=target.renameTo(targetNew);
		if(!r) throw new Exception("File rename error");
		
		logger.end();
	}
	
	
	public LoggerActivityListener getLoggerActivityListener() {
		return logger.getLoggerActivityListener();
	}
	public void setLoggerActivityListener(LoggerActivityListener loggerActivityListener) {	
		logger.setLoggerActivityListener(loggerActivityListener);
	}

}
