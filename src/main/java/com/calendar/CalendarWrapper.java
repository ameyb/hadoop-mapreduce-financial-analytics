package com.gemini.calendar;

import java.util.Calendar;

public class CalendarWrapper extends Calendar{

	/**
	 * idk lol
	 */
	private static final long serialVersionUID = 1L;

	CalendarWrapper(){
		super();
	}
	
	public CalendarWrapper(int month, int day, int year){
		super();
		this.set(Calendar.MONTH, month);
		this.set(Calendar.DATE, day);
		this.set(Calendar.YEAR, year);
	}
	
	@Override
	public boolean equals(Object obj){
		if (obj instanceof Calendar){
			Calendar calObj = (Calendar) obj;
			if ((this.get(Calendar.MONTH) == calObj.get(Calendar.MONTH))
			&&	(this.get(Calendar.DATE) == calObj.get(Calendar.DATE))
			&&	(this.get(Calendar.YEAR) == calObj.get(Calendar.YEAR))){
				return true;
			}
			
		}
		
		return false;
		
	}

	@Override
	@Deprecated
	protected void computeTime() {
		// TODO Auto-generated method stub
		
	}

	@Override
	@Deprecated
	protected void computeFields() {
		// TODO Auto-generated method stub
		
	}

	@Override
	@Deprecated
	public void add(int field, int amount) {
		// TODO Auto-generated method stub
		
	}

	@Override
	@Deprecated
	public void roll(int field, boolean up) {
		// TODO Auto-generated method stub
		
	}

	@Override
	@Deprecated
	public int getMinimum(int field) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	@Deprecated
	public int getMaximum(int field) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	@Deprecated
	public int getGreatestMinimum(int field) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	@Deprecated
	public int getLeastMaximum(int field) {
		// TODO Auto-generated method stub
		return 0;
	}
}
