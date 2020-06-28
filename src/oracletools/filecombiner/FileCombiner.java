package oracletools.filecombiner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

public class FileCombiner {
	
	private String[] files;
	private String targetFileName;
	
	public FileCombiner(String[] files, String targetFileName) {
		super();
		this.files = files;
		this.targetFileName = targetFileName;
	}
	
	public void combine() throws Exception {
		for(int i=1;i<files.length;i++) {
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
		}
		File target=new File(files[0]);
		File targetNew=new File(target.getParent()+"\\\\"+targetFileName);		
		boolean r=target.renameTo(targetNew);
		if(!r) throw new Exception("File rename error");
		
	}

}
