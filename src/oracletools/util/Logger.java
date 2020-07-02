package oracletools.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import oracletools.util.listeners.LoggerActivityListener;

public class Logger {
	
	private LocalDateTime startTime;
	private LocalDateTime prevStepTime;
	private LocalDateTime stepTime;
	private LocalDateTime endTime;
	
	private DecimalFormat decimalFormatWithoutPrecision;
	private DecimalFormat decimalFormatWithPrecision;
	private DateTimeFormatter durDateTimeFormatter;	
	
	private long totalRowCount;
	
	private LoggerActivityListener loggerActivityListener;	
	public LoggerActivityListener getLoggerActivityListener() {
		return loggerActivityListener;
	}
	public void setLoggerActivityListener(LoggerActivityListener loggerActivityListener) {
		this.loggerActivityListener = loggerActivityListener;
	}
	
	private String lastMessage;
	public String getLastMessage() {
		return lastMessage;
	}
	
	private String threadName;

	public String getThreadName() {
		return threadName;
	}
	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}
	
	public Logger(String threadName) {
		
		this.threadName=threadName;
		
		durDateTimeFormatter=DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
		
		//Decimal format for messages
		DecimalFormatSymbols symbols_m=new DecimalFormatSymbols();		
		symbols_m.setGroupingSeparator('.');
		decimalFormatWithoutPrecision=new DecimalFormat("",symbols_m);
		decimalFormatWithoutPrecision.setGroupingUsed(true);
		decimalFormatWithPrecision=new DecimalFormat("#,###.00",symbols_m);
		decimalFormatWithPrecision.setGroupingUsed(true);
		decimalFormatWithPrecision.setDecimalSeparatorAlwaysShown(true);		
		decimalFormatWithPrecision.setMaximumFractionDigits(2);
		
		totalRowCount=0;
	}
	
	public void start() {
		startTime=LocalDateTime.now();
		prevStepTime=startTime;
		lastMessage="Started : " + durDateTimeFormatter.format(startTime);
		
		loggerActivityListener.onLogActivity(threadName,this);
	}
	public void message(String message) {
		LocalDateTime t=LocalDateTime.now();
		lastMessage=message + " : " + durDateTimeFormatter.format(t);
		loggerActivityListener.onLogActivity(threadName,this);
	}
	public void step(long rowCount, String message) {
		stepTime=LocalDateTime.now();
		String stepDiffStr=getDiffTimeString(prevStepTime,stepTime);
		String totalDiffStr=getDiffTimeString(startTime,stepTime);
		
		totalRowCount+=rowCount;
		String rps=Util.rpad(decimalFormatWithPrecision.format(getRps(prevStepTime, stepTime, rowCount)), 14, " ");
		String totalRps=Util.rpad(decimalFormatWithPrecision.format(getRps(startTime, stepTime, totalRowCount)), 14, " ");
		
		String totalRowCountString=Util.rpad(decimalFormatWithoutPrecision.format(totalRowCount),14," ");
		
		lastMessage = "  " + totalRowCountString + message + " | Step Time:" + stepDiffStr + " | Total Time:" + totalDiffStr + " | Rps:" + rps + " | Total Rps:" + totalRps; 
						
		prevStepTime=stepTime;
		
		loggerActivityListener.onLogActivity(threadName,this);
	}
	public void end() {
		endTime=LocalDateTime.now();
		lastMessage="Finished : " + durDateTimeFormatter.format(endTime);
		
		loggerActivityListener.onLogActivity(threadName,this);
	}
	public void error() {
		endTime=LocalDateTime.now();
		lastMessage="Error : " + durDateTimeFormatter.format(endTime);
		
		loggerActivityListener.onLogActivity(threadName,this);
	}
	
	
	private String getDiffTimeString(LocalDateTime startTime,LocalDateTime endTime) {
		Duration diff=Duration.between(startTime, endTime);	
		return Util.lpad(diff.toHoursPart(),2,"0") +":"+Util.lpad(diff.toMinutesPart(),2,"0")+":"+Util.lpad(diff.toSecondsPart(),2,"0");
	}
	private double getRps(LocalDateTime startTime,LocalDateTime endTime, long rowCount) {
		Duration diff=Duration.between(startTime, endTime);
		return (double)rowCount/diff.toSeconds();		
	}
}
