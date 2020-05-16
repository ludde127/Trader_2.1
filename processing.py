import newspaper
import pandas as pd
from __init__ import read_hdf5, files, error, del_hdf5_key, update_hdf5, find_tags_from_str, date_to_posix, tag_str, \
    return_tags
from pprint import pprint
from nltk import tokenize, corpus
import pickle as pkl
import re
import collections
import time
import datetime
import ciso8601
from time import sleep
import numpy as np



special_chars_str = "#~\[]-*“”><:;+-&@"

def trends():
    np3k = newspaper.hot()
    trend = np3k
    return trend

def check_words():

    #twitter = read_hdf5(files["news_store"], "twitter_conversations")
    reddit = read_hdf5(files["news_store"], "reddit")
    words = []
    for index, row in reddit.iterrows():
        score = row["score"]
        title = row["title"]
        text = row["text"]
        all_text = title + " " + text
        urls = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', all_text)

        for char in special_chars_str:
            all_text = all_text.replace(char, "")
        all_text_tokenized = tokenize.word_tokenize(all_text)
        corpus.wordnet.words()
        corp = set(corpus.words.words() + corpus.brown.words())
        english_vocab = set(w.lower() for w in corp)
        sample_lower = set(w.lower() for w in all_text_tokenized if w.lower().isalpha())
        unusual = sample_lower.difference(english_vocab)
        words += unusual
        votes, percent = score.split(":")
        print(votes, all_text, "______", urls)
        print("______________________________")
    print(words)
    counted = collections.Counter(words)
    print(counted)


    with open(files["reddit_comments"], "br") as f:
        d = pkl.load(f)
    pprint(d)
    articles = read_hdf5(files["news_store"], "news_articles")
    print(reddit, articles)
#print(trends())
#check_words()

def tags_history():
    """Create dataframe of tags with their dates formated with the tag as index and a dict with date(hour) and number"""
    dold = read_hdf5(files["news_store"], "news_articles")
    d = read_hdf5(files["news_store"], "news_articles").set_index(keys="created")
    news_created = (d.drop("None").dropna().sort_values(by="created"))
    dold["created"] = dold["created"].apply(date_to_posix)
    print(dold.dropna().set_index(keys="created").sort_values(by="created"))
    sleep(10000)
    dates = news_created.index.values
    print(dates, type(dates))
    stamps = [time.mktime(ciso8601.parse_datetime(t).timetuple()) for t in dates if type(t) is type("")]
    print(stamps)
    for date, stamp in zip(dates, stamps):
        print(f"{date} ; {stamp}")
    sleep(10000)
    twe = read_hdf5(files["news_store"], "twitter_conversations")
    red = read_hdf5(files["news_store"], "reddit").set_index(keys="created")
    print("articles", d)
    print("twitter",twe)
    print("reddit", red)
    texts = 0
    all = {}
    text_list = []

    [text_list.append(t) for t in d.text.values]
    [text_list.append(t) for t in twe.text.values]
    [text_list.append(t) for t in red.selftext.values]

    for p in text_list:
        texts += 1
        print(p)
        tags = find_tags_from_str(p, as_str=False)
        print("TAGS: ", tags)
        for tag in tags:
            if tag in all.keys():
                all[tag] += 1
            else:
                all[tag] = 1
        # from finnhub import Stream
        # Stream()
    pprint(all)

def reddit_comments():
    pic = pd.read_pickle(files["reddit_comments"])
    pprint(pic)

def make_all_timestamp():
    dold = read_hdf5(files["news_store"], "twitter_conversations").reset_index()
    #dold["created_date"] = dold["created_date"].apply(date_to_posix)
    dold = dold.dropna().sort_values(by="created_date")
    print(dold)
    print(dold.loc[dold.index.values[0]])
    update_hdf5(files["news_store"], "twitter_conversations", dataframe=dold.reset_index(), append=False)


def to_backup(file, key, append=True):
    backup_df = read_hdf5(file, key)
    print(backup_df)
    update_hdf5(files["backup"], key, append=append, dataframe=backup_df)


def get_news_tags():
    text_data = []
    r = read_hdf5(files["news_store"], "reddit")
    t = read_hdf5(files["news_store"], "twitter_conversations")
    n = read_hdf5(files["news_store"], "news_articles")
    text_data.extend(r.text.values)
    text_data.extend(r.title.values)
    text_data.extend(t.text.values)
    text_data.extend(n.text.values)
    text_data.extend(n.title.values)

    st = time.process_time()
    all_ = {}
    for text in text_data:
        tags = tag_str(text)
        for tag in tags:
            if tag in all_.keys():
                all_[tag] += 1
            else:
                all_[tag] = 1

    pprint(all_)
    print(f"Took {time.process_time() - st} sek to complete")
    df_tags = pd.DataFrame.from_dict(orient="index", columns=["mentions"], data=all_)
    print(df_tags.sort_values(by="mentions", ascending=False, inplace=True))
    df_tags.to_csv("facts/tags_gotten2.csv")
    # w print Took 96.515625 sek to complete
    # w out print Took 93.09375 sek to complete
    # Took 123.90625 sek to complete


def tags_from_df(df, title=True):
    text_data = []
    text_data.extend(df.text.values)
    if title:
        text_data.extend(df.title.values)
    all_ = {}
    for text in text_data:
        tags = tag_str(text)
        for tag in tags:
            if tag in all_.keys():
                all_[tag] += 1
            else:
                all_[tag] = 1
    all_["tags_combined"] = sum(list(all_.values()))
    all_["time_start"] = min(df.created.values)
    all_["time_stop"] = max(df.created.values)
    return all_


def split_hourly(df):
    newest = max(df.created.values)
    oldest = min(df.created.values)
    hour = 1800
    df_list = []
    hours = 0
    while True:
        border = hour * hours + float(oldest)
        df1 = df[df["created"] < float(border + hour)]
        df1 = df1[df1["created"] > float(border)]
        if len(df1) != 0:
            df_list.append(df1)
            print(max(df1.created.values)-min(df1.created.values))
        hours += 1
        if border > newest:
            break
    return df_list


def change_colum(df, old_col, new_col):
    df[new_col] = df[old_col]
    del df[old_col]
    return df

def dicts_to_df(dicts):
    columns_list = ["tags_combined", "time_start", "time_stop"]
    [columns_list.extend(t) for t in return_tags()]
    df = pd.DataFrame(columns=columns_list)
    for dict in dicts:
        print(dict)
        df = df.append(dict, ignore_index=True).fillna(0)
    print(df)
    return df




if __name__ == "__main__":
    sleep(1000)
    get_news_tags()
    tag = read_hdf5(files["tags_df"], "twitter").AMD.values
    comb = read_hdf5(files["tags_df"], "twitter").tags_combined.values

    for t, c in zip(tag, comb):
        print(t/c)

    sleep(400)
    pprint(return_tags())
    dfs = split_hourly(read_hdf5(files["news_store"], "twitter_conversations"))
    tags_dicts = []
    for df in dfs:
        d = tags_from_df(df, title=False)
        tags_dicts.append(d)
    df_interval = dicts_to_df(tags_dicts)
    update_hdf5(files["tags_df"], "twitter", dataframe=df_interval, append=False)


