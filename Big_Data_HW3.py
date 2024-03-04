import requests
import yaml
import redis
from redis.commands.json.path import Path
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go

APIkey = '4KGGU172X1I01E0E'
#Add ticker options to collect more data (can only make 25 requests daily)
tickerOptions = ['IBM','AAPL']#,'TSLA','NVDA','GOOGL','GOOG']
#IBM,Apple,Tesla,Nvidia,Google(Alphabet),Google

#Create function to take a ticker name and return stock data for an intraday time series
def request_data(stock,APIkey):
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&interval=5min&apikey=%s' % (stock,APIkey)
    stockR = requests.get(url)
    dataR  = stockR.json()
    return dataR

#loading in config parameters from the yaml file
def load_config():
    """Load configuration from the YAML file.

    Returns:
        dict: Configuration data.
    """
    with open("config.yaml", "r") as file:
        return yaml.safe_load(file)
    
config = load_config()

#connecting to the redis DB
def get_redis_connection():
    """Create a Redis connection using the configuration.

    Returns:
        Redis: Redis connection object.
    """
    return redis.Redis(
        host=config["redis"]["host"],
        port=config["redis"]["port"],
        db=0,
        decode_responses=True,
        username=config["redis"]["user"],
        password=config["redis"]["password"],
    )

#method to clear DB out
def flushDB():
    r = get_redis_connection()
    r.flushall()

#method to send data to database
def sendToDB(key,data):
    r = get_redis_connection()
    r.json().set(key, Path.root_path(), data)

#method to retrieve data from database in JSON format
def retrieveData(key):
    r = get_redis_connection()
    data = r.json().get(key)
    return data

#turnToDF takes the number of the ticker item (0 index array) and converts the output 
#of the API to a usable data struct
def turnToDF(num,df):
    # Returns Open Values
    Open = []
    for col in range(7, len(df.columns), 5):
        Open.append(df.iloc[num, col])

    # Returns High Values
    High = []
    for col in range(8, len(df.columns), 5):
        High.append(df.iloc[num, col])

    # Returns Low Values
    Low = []
    for col in range(9, len(df.columns), 5):
        Low.append(df.iloc[num, col])

    # Returns Close Values
    Close = []
    for col in range(10, len(df.columns), 5):
        Close.append(df.iloc[num, col])

    # Returns Vol Values
    Vol = []
    for col in range(11, len(df.columns), 5):
        Vol.append(df.iloc[num, col])

    # dictionary of lists 
    dict = {'Open': Open, 'High': High, 'Low': Low, 'Close': Close, 'Vol': Vol} 
    stockDF = pd.DataFrame(dict)
    stockDF.dropna(inplace=True)
    return stockDF

# create array for results to be saved
results = []
#clear DB before entering new data
flushDB()

#looping through each ticker option in array of strings (ln 12) and saving to array
for ii in tickerOptions:
        data = request_data(ii,APIkey)
        sendToDB(ii,data)
        results.append(retrieveData(ii))

#turning results into a dataframe using pandas
df = pd.json_normalize(results)

#due to complexity of data output, saving to csv first to organize
df.to_csv('stockData.csv')

#opening saved CSV 
stockData = pd.read_csv('stockData.csv')

#List each index for corresponding ticker options in tickerOptions
IBMdf  = turnToDF(0,stockData)
AAPLdf = turnToDF(1,stockData)

#Finding max value of data
maxIBM  =  IBMdf.loc[IBMdf['High'].idxmax()]
print('The max stock price of IBM today was %i',maxIBM)
maxAAPL =  AAPLdf.loc[AAPLdf['High'].idxmax()]
print('The max stock price of AAPL today was %i',maxAAPL)

#Finding min value of data
minIBM  =  IBMdf.loc[IBMdf['Low'].idxmax()]
print('The lowest stock price of IBM today was %i',minIBM)
minAAPL =  AAPLdf.loc[AAPLdf['Low'].idxmax()]
print('The lowest stock price of AAPL today was %i',minAAPL)

#plotting candlestick plot for each ticker item
#for additional items, add additional figures
fig = go.Figure(data=[go.Candlestick(x=IBMdf.index,
                                    open=IBMdf['Open'],
                                    high=IBMdf['High'],
                                    low=IBMdf['Low'],
                                    close=IBMdf['Close'])])

fig.show()

fig2 = go.Figure(data=[go.Candlestick(x=AAPLdf.index,
                                    open=AAPLdf['Open'],
                                    high=AAPLdf['High'],
                                    low=AAPLdf['Low'],
                                    close=AAPLdf['Close'])])

fig2.show()
