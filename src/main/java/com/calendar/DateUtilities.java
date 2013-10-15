package com.gsi.calendar;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.LocalDate;

public class DateUtilities {
	
	public static LocalDate getJodaDate(String dateString) throws ParseException{
		String[] dateStringArray = dateString.split("-");
		int year = Integer.parseInt(dateStringArray[0]);
		int month = Integer.parseInt(dateStringArray[1]);
		int day = Integer.parseInt(dateStringArray[2]);
		
		LocalDate date = new LocalDate(year, month, day);
		return date;
	}
}
