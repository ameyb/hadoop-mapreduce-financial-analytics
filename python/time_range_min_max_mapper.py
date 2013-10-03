#!/usr/bin/env python

import sys
from datetime import datetime,date,timedelta

def read_input_line():
	for line in sys.stdin:
		yield line

def main():
	try:
		date_range_start = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
		date_range_end = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
	except:
		date_range_end = date.today()
		date_range_start = date_range_end - timedelta(days=7*52) 
	
	
	#print date_range_start,  date_range_end
	
	stocks = {}
	for line in read_input_line():
		line = line.strip()
		records = line.split(',')
		try:
			stock = str(records[1])
			date_close = datetime.strptime(records[2], "%Y-%m-%d").date()
			price_close = float(records[6])
		except ValueError:
			#print 'data error : ', records
			continue
		if date_close < date_range_start or date_close > date_range_end:
			continue
		if stock in stocks:
			stocks[stock]["price_close_min"] = min(stocks[stock]["price_close_min"], price_close)
			stocks[stock]["price_close_max"] = max(stocks[stock]["price_close_min"], price_close)
		else:
			stocks[stock] = {
					"price_close_min": price_close,
					"price_close_max": price_close,
					}
	
	for stock in stocks:
		print "%s\t%s" %(stock,stocks[stock]["price_close_min"])
		print "%s\t%s" %(stock,stocks[stock]["price_close_max"])



if __name__ == "__main__":
    main()





#exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
#NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24
