#!/usr/bin/env python

import sys
from datetime import datetime,date,timedelta

def read_input_line():
        for line in sys.stdin:
                yield line

def main():
	
	
	stocks = {}
	for line in read_input_line():
		line = line.strip()
		records = line.split('\t')
		try:
			stock = str(records[0])
			price = float(records[1])
		except ValueError:
			#print 'data error : ', records
			continue
		if stock in stocks:
			stocks[stock]["price_close_min"] = min(stocks[stock]["price_close_min"], price)
			stocks[stock]["price_close_max"] = max(stocks[stock]["price_close_min"], price)
		else:
			stocks[stock] = {
					"price_close_min": price,
					"price_close_max": price,
					}
	
	for stock in stocks:
		print "%s\t%s" %(stock,stocks[stock]["price_close_min"])
		print "%s\t%s" %(stock,stocks[stock]["price_close_max"])



if __name__ == "__main__":
    main()





#exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
#NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24
