package oracletools.util;

import java.util.HashMap;
import java.util.Map;

public class MultithreadMessaging {
	
	private static HashMap<String, Logger> loggers;
	private static HashMap<String, Boolean> errors;
	private static boolean hasError=false;
	
	
	public static void onThreadLogActivity(String threadName,Logger logger) {
		if(loggers==null) {
			loggers=new HashMap<String, Logger>();
		}		
		loggers.put(threadName, logger);
		
		String str="";
		if(errors.get(threadName)) {
			str+="ERROR:";
		}
		for (Map.Entry<String, Logger> entry : loggers.entrySet() ) {
		    str+=entry.getKey()+" - ";
		    str+=entry.getValue().getLastMessage();
		    str+=" || ";
		}
		System.out.println(str);
		
	}
	
	public static void onThreadError(String threadName, Exception e) {
		if(errors==null) {
			errors=new HashMap<String, Boolean>();
		}
		errors.put(threadName, true);
		
		if(!hasError) {
			hasError=true;
		}
		
		e.printStackTrace();
	}
	
}
