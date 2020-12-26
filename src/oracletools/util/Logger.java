package oracletools.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
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
		decimalFormatWithoutPrecision=new DecimalFormat("#,###",symbols_m);
		decimalFormatWithoutPrecision.setGroupingUsed(true);
		decimalFormatWithoutPrecision.setDecimalSeparatorAlwaysShown(false);
		decimalFormatWithPrecision=new DecimalFormat("#,###.00",symbols_m);
		decimalFormatWithPrecision.setGroupingUsed(true);
		decimalFormatWithPrecision.setDecimalSeparatorAlwaysShown(true);		
		decimalFormatWithPrecision.setMaximumFractionDigits(2);
		
		totalRowCount=0;
	}
	
	public void start() {
		if (loggerActivityListener != null) {
			startTime = LocalDateTime.now();
			prevStepTime = startTime;
			lastMessage = "Started : " + durDateTimeFormatter.format(startTime);
			lastMessage = Util.rpad(lastMessage, 60, " ");

			loggerActivityListener.onLogActivity(threadName, this);
		}
	}
	public void message(String message) {
		if (loggerActivityListener != null) {
			LocalDateTime t = LocalDateTime.now();
			lastMessage = message + " : " + durDateTimeFormatter.format(t);
			lastMessage = Util.rpad(lastMessage, 60, " ");
			loggerActivityListener.onLogActivity(threadName, this);
		}
	}
	public void step(long rowCount, String message) {
		if (loggerActivityListener != null) {
			stepTime = LocalDateTime.now();
			String stepDiffStr = Util.getDiffTimeString(prevStepTime, stepTime);
			String totalDiffStr = Util.getDiffTimeString(startTime, stepTime);

			totalRowCount += rowCount;
			String rps = Util.rpad(decimalFormatWithoutPrecision.format(Util.getRps(prevStepTime, stepTime, rowCount)), 7,
					" ");
			String totalRps = Util
					.rpad(decimalFormatWithoutPrecision.format(Util.getRps(startTime, stepTime, totalRowCount)), 7, " ");

			String totalRowCountString = Util.rpad(decimalFormatWithoutPrecision.format(totalRowCount), 14, " ");

			lastMessage = "" + totalRowCountString /* + message */ + " | Dur:" + stepDiffStr + "/" + totalDiffStr
					+ " | Rps:" + rps + "/" + totalRps;

			prevStepTime = stepTime;

			loggerActivityListener.onLogActivity(threadName, this);
		}
	}
	public void end() {
		if (loggerActivityListener != null) {
			endTime = LocalDateTime.now();
			lastMessage = "Finished : " + durDateTimeFormatter.format(endTime);
			lastMessage = Util.rpad(lastMessage, 60, " ");

			loggerActivityListener.onLogActivity(threadName, this);
		}
	}
	public void error() {
		if (loggerActivityListener != null) {
			endTime = LocalDateTime.now();
			lastMessage = "Error : " + durDateTimeFormatter.format(endTime);
			lastMessage = Util.rpad(lastMessage, 60, " ");

			loggerActivityListener.onLogActivity(threadName, this);
		}
	}
	
	
	
}
