package oracletools.util;

import java.time.Duration;
import java.time.LocalDateTime;

public class Util {
	public static String lpad(int str,int length,String fill) {
		return lpad(str+"",length,fill);
	}
	public static String lpad(String str,int length,String fill) {
		int stringLength=str.length();
		if(stringLength>=length) {
			return str;
		}
		else {
			StringBuilder sb=new StringBuilder();
			for(int i=0;i<length-stringLength;i++) {
				sb.append(fill);
			}
			sb.append(str);
			return sb.toString();
		}
	}
	public static String rpad(int str,int length,String fill) {
		return rpad(str+"",length,fill);
	}
	public static String rpad(String str,int length,String fill) {
		int stringLength=str.length();
		if(stringLength>=length) {
			return str;
		}
		else {
			StringBuilder sb=new StringBuilder();
			sb.append(str);			
			for(int i=0;i<length-stringLength;i++) {
				sb.append(fill);
			}			
			return sb.toString();
		}
	}
	public static String getDiffTimeString(LocalDateTime startTime,LocalDateTime endTime) {
		Duration diff=Duration.between(startTime, endTime);	
		return Util.lpad(diff.toHoursPart(),2,"0") +":"+Util.lpad(diff.toMinutesPart(),2,"0")+":"+Util.lpad(diff.toSecondsPart(),2,"0");
	}
	public static double getRps(LocalDateTime startTime,LocalDateTime endTime, int rowCount) {
		Duration diff=Duration.between(startTime, endTime);
		return (double)rowCount/diff.toSeconds();		
	}

}
