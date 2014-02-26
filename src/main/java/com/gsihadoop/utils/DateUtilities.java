package com.gsihadoop.utils;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DateUtilities {
	public static SimpleDateFormat FORMAT_FULL = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * @param dateString if dateString="yyyy-MM-dd", as necessary add more date formats
	 * @return
	 * @throws ParseException
	 */
	public static Date getDate(String dateString) throws ParseException{
		if (dateString.matches("([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})")){
			return FORMAT_FULL.parse(dateString);
		} else {
			return new Date();
		}
		
	}
}
