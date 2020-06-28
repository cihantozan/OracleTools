package oracletools.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
	
	private LocalDateTime startTime;
	private LocalDateTime prevStepTime;
	private LocalDateTime stepTime;
	private LocalDateTime endTime;
	
	private DecimalFormat decimalFormatWithoutPrecision;
	private DecimalFormat decimalFormatWithPrecision;
	private DateTimeFormatter durDateTimeFormatter;	
	
	private long totalRowCount;
	
	public Logger() {
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
		print("Started : " + durDateTimeFormatter.format(startTime));
	}
	public void message(String message) {
		LocalDateTime t=LocalDateTime.now();
		print(message + " : " + durDateTimeFormatter.format(t));
	}
	public void step(long rowCount, String message) {
		stepTime=LocalDateTime.now();
		String stepDiffStr=getDiffTimeString(prevStepTime,stepTime);
		String totalDiffStr=getDiffTimeString(startTime,stepTime);
		
		totalRowCount+=rowCount;
		String rps=Util.rpad(decimalFormatWithPrecision.format(getRps(prevStepTime, stepTime, rowCount)), 14, " ");
		String totalRps=Util.rpad(decimalFormatWithPrecision.format(getRps(startTime, stepTime, totalRowCount)), 14, " ");
		
		String totalRowCountString=Util.rpad(decimalFormatWithoutPrecision.format(totalRowCount),14," ");
		
		String printMessage = "  " + totalRowCountString + message + " | Step Time:" + stepDiffStr + " | Total Time:" + totalDiffStr + " | Rps:" + rps + " | Total Rps:" + totalRps; 
		
		print(printMessage);
		
		prevStepTime=stepTime;
	}
	public void end() {
		endTime=LocalDateTime.now();
		print("Finished : " + durDateTimeFormatter.format(endTime));
	}
	
	private void print(String str) {
		System.out.println(str);
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
