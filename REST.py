# Ni Xiaoxi Python Coding Exercise
import pandas_datareader.data as web
import pymysql.cursors
import pandas as pd
import numpy as np
import seaborn as sns; sns.set()
import pylab
import datetime
import holidays

ONE_DAY = datetime.timedelta(days=1)
# assume US holidays for illustration only
HOLIDAYS_US = holidays.US()

# to get +0BD
def next_business_day(current_date):
    next_day = current_date
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_US:
        next_day += ONE_DAY
    return next_day
# to get -0BD
def prev_business_day(current_date):
    prev_day = current_date
    while prev_day.weekday() in holidays.WEEKEND or prev_day in HOLIDAYS_US:
        prev_day -= ONE_DAY
    return prev_day

# function to get the data from yahoo and update the database
def get_historical_data(ticker_symbol, start, end):
    # connect to local database using MySQL server
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='1122',
                                 db='REST')
    try:
        with connection.cursor() as cursor:
            # two tables created in the database REST
            # #1 table TICKERS stores ticker id and ticker name
            # #2 table PRICES stores date, ticker id, open, high, low, close and volume info
            sql = "SELECT id, name FROM TICKERS"
            cursor.execute(sql)
            result = cursor.fetchall()
            ticker_list = [x[1] for x in result]
            # to check if the requested ticker is already in the TICKERS
            if ticker_symbol not in ticker_list:
                # return a list of available tickers
                print("Available Tickers:", ticker_list)
                try:
                    # insert the new ticker in the database
                    print("Updating Database ......")
                    sql = "INSERT INTO TICKERS (name) VALUES (%s)"
                    cursor.execute(sql, (ticker_symbol,))
                    connection.commit()
                    # get the generated ticker id
                    sql = "SELECT id FROM TICKERS WHERE name = %s"
                    cursor.execute(sql, (ticker_symbol,))
                    ticker_id = cursor.fetchone()[0]
                except:
                    print("Database Error")
            else:
                ticker_id =result[ticker_list.index(ticker_symbol)][0]
            
            # get info from public source, i.e. yahoo, and store new info in DB
            try:
                # in case the requested date is a weekend/holiday
                start = next_business_day(start)
                end = prev_business_day(end)
                # get the data using pandas_datareader
                prices = web.DataReader(ticker_symbol, 'yahoo', start, end)
                sql = "SELECT date, close FROM PRICES WHERE ticker_id = %s"
                cursor.execute(sql, (ticker_id,))
                result = cursor.fetchall()
                date_list = [x[0] for x in result]
                # to check if for requested dates, there are prices stored in DB
                for i in range(len(prices)):
                    sql_data = {
                        'ticker_id': ticker_id,
                        'date': prices.index[i].date(),
                        'open': float(prices.ix[i][0]),
                        'high': float(prices.ix[i][1]),
                        'low': float(prices.ix[i][2]),
                        'close': float(prices.ix[i][3]),
                        'volume': float(prices.ix[i][5]),
                    }
                    # in the case of a new date, insert the price into DB
                    if prices.index[i].date() not in date_list:
                        sql = "INSERT INTO PRICES (ticker_id, date, open, high, low, close, volume) VALUES (%(ticker_id)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)"
                        cursor.execute(sql, sql_data)
                        connection.commit()
                    else:
                        # get the close price (use close price to identify if there is any update on price)
                        close_price = result[date_list.index(prices.index[i].date())][1]
                        # to compare new close price vs. existing close price
                        if abs(float(close_price) - float(prices.ix[i][3])) > 1E-6:
                            # if price changes, then update the DB
                            sql = "UPDATE PRICES SET open = %(open)s, high = %(high)s, low = %(low)s, close = %(close)s, volume = %(volume)s WHERE ticker_id = %(ticker_id)s and date = %(date)s"
                            cursor.execute(sql, sql_data)
                            connection.commit()
                return prices

            except pymysql.InternalError as error:
                print("No Prices Updated for Ticker {}".format(ticker_symbol))
                print(error.args)

    finally:
        connection.close()

# to use the get_historical_data function and back test a simple strategy
if __name__ == '__main__':
    # specify start date
    start = datetime.date(2013, 10, 30)
    # specify end date
    end = datetime.date(2016, 10, 30)
    # specify a ticker
    ticker_symbol = 'BABA'
    # get prices and sync the DB
    prices = get_historical_data(ticker_symbol, start, end)
    # convert into a pandas DataFrame object
    df = pd.DataFrame(prices['Close'])
    # compute the return
    df['Return'] = np.log(df['Close']/df['Close'].shift(1))
    cols = []
    # to back test the momentum strategy, i.e. to take the mean return over the last 15, 30, or 60 days
    # if the value is positive, go/stay long
    # if the value is negative, go/stay short
    for momentum in [15, 30, 60]:
        col = 'position_%s' % momentum
        df[col] = np.sign(df['Return'].rolling(momentum).mean())
        cols.append(col)
    
    # to derive the absolute performance of the strategy for the different time intervals
    strats = ['Return']
    for col in cols:
        strat = 'strategy_%s' % col.split('_')[1]
        # multiply the positionings derived above by the market return
        df[strat] = df[col].shift(1) * df['Return']
        strats.append(strat)
    
    # plot the outcome
    # the one based on 30 days outperforms others
    df[strats].dropna().cumsum().apply(np.exp).plot()
    pylab.show()