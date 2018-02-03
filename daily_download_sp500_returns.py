import pickle
import urllib2
import pytz
import pandas as pd
import time
from bs4 import BeautifulSoup
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime
from pandas_datareader import DataReader


WIKI_SP500_LIST_URL = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

sector = 'consumer_discretionary'
returns = {}

VANTAGE_API_KEY = '7SRT21LXFZIL2YI3'
weird_vantage_string = '4. close'
SLEEP_TIME = 10


def scrape_list(site):
    # Thanks to Algo Engineer (http://www.thealgoengineer.com/2014/download_sp500_data/)
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = urllib2.Request(site, headers=hdr)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
              ticker = str(col[0].string.strip())
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    for k, v in sector_tickers.iteritems():
        sector_tickers[k] = sorted(sector_tickers[k])
    return sector_tickers


def dict_to_df(d):
    assert isinstance(d, dict)
    longest = 0
    for k, v in d.iteritems():
        L = len(d[k])
        longest = max(longest, L)

    for k, v in d.iteritems():
        L = len(d[k])
        diff = longest - L
        if diff > 0:
            for _ in range(diff):
                d[k].append('')
    return pd.DataFrame(d)


if __name__ == '__main__':
    print 'Scraping wiki site for SP500 ticker names...'
    ticks = scrape_list(WIKI_SP500_LIST_URL)
    print 'Scrape complete!'

    try:
        print 'Saving tickers to csv...'
        dict_to_df(ticks).to_csv('sp500_ticks.csv')
        print 'Tickers saved!'
    except:
        pass

    print 'Downloading returns from Vantage...'
    ts = TimeSeries(key=VANTAGE_API_KEY, output_format='pandas')
    with open('temp_output.pkl', 'wb') as temp_out:
        k = 0
        for symbol in ticks[sector]:
            print 'Working on: ' + symbol
            data, meta_data = ts.get_daily_adjusted(symbol=symbol, outputsize='full')
            data.index = pd.DatetimeIndex(data.index)
            data[weird_vantage_string] = data[weird_vantage_string].pct_change()
            returns[symbol] = data[weird_vantage_string]
            pickle.dump(returns, temp_out)
            k += 1
            if k % 5 == 0:
                # Save intermediate results just in case...
                # You can do a 'rm [0-9]*' later to remove
                returns_df = pd.DataFrame(returns)
                returns_df.to_csv(str(k) + '_' + sector + '_daily_adj_returns.csv')

                print 'System pausing....zzzzzz'
                time.sleep(SLEEP_TIME)

    print 'Donwload complete. Saving results...'
    returns_df = pd.DataFrame(returns)
    returns_df.to_csv(sector + '_daily_adj_returns.csv')
    print 'Success!'
