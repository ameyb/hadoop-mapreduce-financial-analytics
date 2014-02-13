package com.gsihadoop.utils;

import java.text.ParseException;

public class StockData {

	private String exchange;
	private String stock_symbol;
	private String date;
	private double stock_price_open;
	private double stock_price_high;
	private double stock_price_low;
	private double stock_price_close;
	private int stock_volume;
	private double stock_price_adj_close;

	/**
	 * Make this constructor private to force use of the factory parse record.
	 */
	private StockData(){
		setExchange("");
		setStock_symbol("");
		date = null;
		stock_price_open = 0.00;
		stock_price_high =0.00;
		stock_price_low = 0.00;
		stock_price_close = 0.00;
		stock_volume = 0;
		stock_price_adj_close = 0.00;
	}
	
	public static StockData parse(String csvRow){
		StockData record = new StockData();
		String[] values = csvRow.split(",", -1);

		/*if (values.length != 9) {
			return null;
		}*/
			
		try {
			record.setExchange(values[0].trim());
			record.setStock_symbol(values[1].trim());
			record.setDate(values[2].trim());
			record.stock_price_high = Double.parseDouble(values[4].trim());
			record.stock_price_low = Double.parseDouble(values[5].trim());
			record.stock_price_open = Double.parseDouble(values[3].trim());
			record.stock_price_close = Double.parseDouble(values[6].trim());
			record.stock_volume = Integer.parseInt(values[7].trim());
			record.stock_price_adj_close = Double.parseDouble(values[8].trim());
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
		} catch(NumberFormatException e){
			e.printStackTrace();
		} 
		
		return record;
	}
	
	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public String getStock_symbol() {
		return stock_symbol;
	}

	public void setStock_symbol(String stock_symbol) {
		this.stock_symbol = stock_symbol;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String tempDate) {
		this.date = tempDate;
	}

	public double getStock_price_high() {
		return stock_price_high;
	}

	public void setStock_price_high(double stock_price_high) {
		this.stock_price_high = stock_price_high;
	}

	public double getStock_price_low() {
		return stock_price_low;
	}

	public void setStock_price_low(double stock_price_low) {
		this.stock_price_low = stock_price_low;
	}
		
	public double getStock_price_close() {
		return stock_price_close;
	}

	public void setStock_price_close(double stock_price_close) {
		this.stock_price_close = stock_price_close;
	}
}