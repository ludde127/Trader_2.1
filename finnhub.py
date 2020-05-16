import websocket
import requests
import yaml
import time
import datetime
import pandas as pd
from pprint import pprint
import ast
from __init__ import read_yaml, files, read_txt, symbols, update_hdf5, read_hdf5
import pathlib
api_key = read_yaml(files["auth"])["finnhub_api"]





def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")

def parse_responses(url):
    data = {}
    r = requests.get(url)
    tries = 0
    while True:
        try:
            as_json = r.json()

            break
        except Exception as e:
            print(e)
            tries += 1
            time.sleep(2)
        if tries == 2:
            as_json = {}
            break
        time.sleep(1)

    return as_json



class Stream:
    def __init__(self, added_trace_symbols=(), overwrite_trace_syms=False):
        url = f"wss://ws.finnhub.io?token={api_key}"
        self.add_syms = added_trace_symbols
        self.overwrite_syms = overwrite_trace_syms
        self.dataframe_dict = {}

        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(url, on_message=self.on_message, on_error=on_error, on_close=on_close)
        ws = self.ws

        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def on_open(self):
        syms = symbols(added=self.add_syms, overwrite_syms_with_added=self.overwrite_syms)
        print(syms)

        for sym in syms:
            str = "{" + f'"type":"subscribe", "symbol":"{sym}"' + "}"
            self.ws.send(str)
            columns = ["time", "price", "volume"]
            self.dataframe_dict[sym] = pd.DataFrame(columns=columns)

    def on_message(self, message):
        """Get message, add to dataframes and each 15 updates save it to drive"""

        mess = ast.literal_eval(message)
        if mess["type"] == "trade":
            print(message)
            data = mess["data"][0]
            rel_sym = data["s"]
            price = data["p"]
            time = data["t"]
            volume = data["v"]
            info_dict = {"time": float(time), "price": float(price), "volume": float(volume)}
            self.dataframe_dict[rel_sym] = self.dataframe_dict[rel_sym].append(info_dict, ignore_index=True)
            if len(self.dataframe_dict[rel_sym].index.values) % 357 == 0:
                print(self.dataframe_dict[rel_sym])
                # Time is correct but does not show //
                # print(f"first_time: {self.dataframe_dict[rel_sym]['time'].values[0]}, last: {self.dataframe_dict[rel_sym]['time'].values[-1]}")
                file = files["streamed_data_finnhub"]
                update_hdf5(file, key=rel_sym, dataframe=self.dataframe_dict[rel_sym])
                self.dataframe_dict[rel_sym] = self.dataframe_dict[rel_sym].iloc[0:0]


class CompanyInfo:
    def __init__(self, syms=(), only_given_syms=False):
        self.syms = symbols(syms, only_given_syms)

    def company_profile(self):
        profiles = {}

        for sym in self.syms:
            if len(sym) <= 6:
                url = f'https://finnhub.io/api/v1/stock/profile?symbol={sym}&token={api_key}'
                profiles[sym] = parse_responses(url)
        return profiles

    def recommendation_trends(self):
        recomendations = {}
        for sym in self.syms:
            url = f'https://finnhub.io/api/v1/stock/recommendation?symbol={sym}&token={api_key}'
            recomendations[sym] = parse_responses(url)
        return recomendations

    def price_target(self):
        targets = {}
        for sym in self.syms:
            url = f'https://finnhub.io/api/v1/stock/price-target?symbol={sym}&token={api_key}'
            targets[sym] = parse_responses(url)
        return targets

    def down_up(self):
        d_u = {}
        for sym in self.syms:
            url = f"https://finnhub.io/api/v1/stock/upgrade-downgrade?symbol={sym}&token={api_key}"
            d_u[sym] = parse_responses(url)
        return d_u

    def earnings(self):
        earnings = {}
        for sym in self.syms:
            url = f'https://finnhub.io/api/v1/stock/earnings?symbol={sym}&token={api_key}'
            earnings[sym] = parse_responses(url)
        return earnings

    def metrics(self, metric):
        metrics = {}
        mets = ["price", "valuation", "growth", "margin", "management", "financialStrength", "perShare"]
        if metric in mets:
            for sym in self.syms:
                url = f'https://finnhub.io/api/v1/stock/metric?symbol={sym}&metric={metric}&token={api_key}'
                metrics[sym] = parse_responses(url)

            return metrics
        else:
            print(f"invalid metric '{metric}, must be {mets}'")
            return f"invalid metric '{metric}, must be {mets}'"

    def historicalcandeldata(self, from_date=None, to=round(time.time()), res=1):
        print("Doesent work probably because of to large a size")
        historicaldata = {}
        if from_date:
            from_date = round(from_date.timestamp())
        else:
            from_date = round(to) - 20*360*24*3600
        for sym in self.syms:
            print(sym)
            print(res)
            print(from_date)
            print(to)
            url = f"https://finnhub.io/api/v1/stock/candle?symbol={sym}&resolution={res}&from={from_date}&to={to}&format=json&token={api_key}'"
            print(url)
            historicaldata[sym] = parse_responses(url)
            print(historicaldata)


class FinnNews:
    def __init__(self, syms=(), only_given_syms=False):
        self.syms = symbols(syms, only_given_syms)
        self.syms = [sym for sym in syms if ":" not in sym]
        self.syms.remove("VIX")

    def generalnews(self, category="general", min_id=0):
        if not min_id:
            url = f'https://finnhub.io/api/v1/news?category={category}&token={api_key}'
        else:
            url = f'https://finnhub.io/api/v1/news?category={category}&minId={min_id}&token={api_key}'
        news = parse_responses(url)
        return news

    def companynews(self):
        news = {}
        for sym in self.syms:
            url = f"https://finnhub.io/api/v1/news/{sym}?token={api_key}"
            news[sym] = parse_responses(url)
        return news

    def mayor_developments(self):
        developments = {}
        for sym in self.syms:
            url = f'https://finnhub.io/api/v1/major-development?symbol={sym}&token={api_key}'
            developments[sym] = parse_responses(url)
        return developments

    def news_sentiment(self):
        sentiments = {}
        for sym in self.syms:
            url = f'https://finnhub.io/api/v1/news-sentiment?symbol={sym}&token={api_key}'
            sentiments[sym] = parse_responses(url)
        return sentiments





if __name__ == "__main__":
    #data = read_hdf5(files["streamed_data_finnhub"], "AMD")
    #print(data.volume.values.mean())
    #profiles = FinnNews.news_sentiment(FinnNews())
    profiles = CompanyInfo.earnings(CompanyInfo(syms="SNAP", only_given_syms=True))
    pprint(profiles)
    #print(read_hdf5(files["streamed_data_finnhub"], "BINANCE:BTCUSDT"))


