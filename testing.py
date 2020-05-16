from __init__ import read_hdf5, files, find_tags_from_str
from pprint import pprint
import pandas as pd
from time import sleep

reddit_df = read_hdf5(files["news_store"], "reddit")
print(reddit_df)
print(reddit_df.columns)

print(reddit_df.selftext.values)
for t in reddit_df.title.values:
    if len(t) > 15:
        print(t)
        print("tags; ", find_tags_from_str(t, as_str=True))
