package com.gsihadoop;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class StockData {

	public String exchange;
	public String stock_symbol;
	public long date;
	public String stock_price_open = "";
	public String stock_price_high = "";
	public String stock_price_low = "";
	public String stock_price_close = "";
	public String stock_volume = "";
	public String stock_price_adj_close = "";

	private static final String DATE_FORMAT = "yyyy-MM-dd";

	private static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

	private static final String DATE_FORMAT_YYMM = "yyyyMM";

	private static SimpleDateFormat sdf_ym = new SimpleDateFormat(
			DATE_FORMAT_YYMM);

	public String getYearMonth() {

		return sdf_ym.format(this.date);

	}

	public String getDate() {

		return sdf.format(this.date);

	}

	public float getClose() {

		return Float.parseFloat(this.stock_price_close);

	}

	public float getAdjustedClose() {

		return Float.parseFloat(this.stock_price_adj_close);

	}

	public static StockData parse(String csvRow) {
		StockData record = new StockData();
		String[] values = csvRow.split(",", -1);

		if (values.length != 9) {
			return null;
		}

		record.exchange = values[0].trim();
		record.stock_symbol = values[1].trim();
		String n_Date = values[2].trim();
		try {
			record.date = sdf.parse(n_Date).getTime();

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		record.stock_price_open = values[3].trim();
		record.stock_price_high = values[4].trim();
		record.stock_price_low = values[5].trim();
		record.stock_price_close = values[6].trim();
		record.stock_volume = values[7].trim();
		record.stock_price_adj_close = values[8].trim();

		return record;
	}
}

