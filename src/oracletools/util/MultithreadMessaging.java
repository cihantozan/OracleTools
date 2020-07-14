package oracletools.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class MultithreadMessaging {
	
	private static HashMap<String, Logger> loggers;
	private static HashMap<String, Boolean> errors;
	private static boolean hasError=false;
	
	
	public static synchronized void onThreadLogActivity(String threadName,Logger logger) {
		if(loggers==null) {
			loggers=new LinkedHashMap<String, Logger>();
		}		
		loggers.put(threadName, logger);
		
		if(errors==null) {
			errors=new HashMap<String, Boolean>();
		}
		if(!errors.containsKey(threadName)) {
			errors.put(threadName, false);
		}
		
		
		
		
		String str="";
		if(errors.get(threadName)) {
			str+="ERROR:";
		}
		for (Map.Entry<String, Logger> entry : loggers.entrySet() ) {
			
			String name=entry.getKey();
			if(name.length()>15) {
				name=name.substring(name.length()-15, name.length()-1);
			}
			else {
				name=Util.rpad(name, 15, " ");
			}
			
		    str+=name+" - ";
		    if(errors.get(entry.getKey())) {
		    	str+= "ERROR ";
		    }
		    str+=entry.getValue().getLastMessage();
		    str+=" || ";
		}
		System.out.println(str);
		
	}
	
	public static synchronized void onThreadError(String threadName, Exception e) {
		if(errors==null) {
			errors=new HashMap<String, Boolean>();
		}
		errors.put(threadName, true);
		
		if(!hasError) {
			hasError=true;
		}
		
		e.printStackTrace();
	}
	
	public static void reset() {
		loggers=null;
		errors=null;
	}
	
}
