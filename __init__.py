import pathlib
import os
from nltk import corpus, word_tokenize
import datetime
from googletrans import Translator
import pandas as pd
import h5py
import newspaper
import json
from time import sleep
from pprint import pprint
import time
import ciso8601
import yaml
import pickle

def makefolders(folders):
    for folder in folders:
        os.makedirs(folder, exist_ok=True)

def news_tags(added=()):
    syms = pd.read_csv(files["all_symbols"])
    cities = pd.read_csv(files["city_gdp"])
    city_tags = cities["City"].tolist() + cities["Country"].tolist()
    all_tags = [w.lower() for w in syms["Name"].tolist()] + [w.lower() for w in syms["Sector"].tolist()]
    all_tags = set(all_tags + [c.lower() for c in city_tags])
    return all_tags

local_dir = pathlib.Path(__file__).parent.absolute()
folders = (pathlib.Path("finnhub"), pathlib.Path("news"), pathlib.Path("troubleshoot"))
makefolders(folders)
files = {"auth": "auth.yaml",
         "trace_syms": pathlib.Path("finnhub/symbols.txt"),
         "streamed_data_finnhub": pathlib.Path("finnhub/stream_data.h5"),
         "log": pathlib.Path("troubleshoot/log.txt"),
         "error": pathlib.Path("troubleshoot/error.txt"),
         "news_store": pathlib.Path("news/news_data.h5"),
         "news_keywords": pathlib.Path("news/keywords.txt"),
         "subreddits": pathlib.Path("news/subreddits.txt"),
         "reddit_comments": pathlib.Path("news/reddit_comment_pickle.pkl"),
         "company_info": pathlib.Path("facts/company_info.csv"),
         "city_list": pathlib.Path("facts/city_list.json"),
         "all_symbols": pathlib.Path("facts/all_symbols.csv"),
         "tags_txt": pathlib.Path("facts/tags.txt"),
         "city_gdp": pathlib.Path("facts/city_gdp.csv"),
         "twitter_tags": pathlib.Path("facts/twitter_tags_limited.txt"),
         "backup": pathlib.Path("backup.h5"),
         "tags_df": pathlib.Path("news/tags.h5"),
         "memory": pathlib.Path("memory.yaml"),
         "tags_pickle": pathlib.Path("news/tags_pickle.pkl")}

for key in files.keys():
    files[key] = local_dir.joinpath(files[key])


def read_yaml(file, safe=True):
    with open(file, "r") as f:
        if safe:
            data = yaml.safe_load(f)
        else:
            data = yaml.unsafe_load(f)
    return data


def update_yaml(file, dict):
    with open(file, "w") as f:
        yaml.safe_dump(dict, f)



def read_txt(file):
    try:
        with open(file, "r") as f:
            data = f.readlines()
            return data
    except FileNotFoundError as e:
        return []


def symbols(added=(), overwrite_syms_with_added=False):
    if overwrite_syms_with_added:
        if type(added) == str():
            syms = [added]
        else:
            syms = [added]
    else:
        syms = read_txt(files["trace_syms"])
    if added:
        syms += added
    syms = [sym.upper().replace("\n", "") for sym in syms]
    return syms


def check_lang_is_en(sample, lenght_req=True, safe=False, confidence=0.7):

    sample = str(sample.replace("\n", " ").replace('"', "").replace("<", "").replace(">", ""))
    try:
        if not safe:
            prediction = Translator().detect(sample)
            if prediction.confidence >= confidence and prediction.lang == "en":
                return True
        else:
            corp = set(corpus.words.words() + corpus.brown.words())
            english_vocab = set(w.lower() for w in corp)
            sample = word_tokenize(sample)
            sample_lower = set(w.lower() for w in sample if w.lower().isalpha())
            unusual = sample_lower.difference(english_vocab)
            if lenght_req:
                if len(unusual) > (len(sample) * 0.2) or len(sample) < 35:
                    # print(f"Blocked: {sample_org} \n")
                    return False
                else:
                    # print(f"FINE: {sample_org} \n")
                    return True
            else:
                if len(unusual) > (len(sample) * 0.2):
                    # print(f"Blocked: {sample_org} \n")
                    return False
                else:
                    # print(f"FINE: {sample_org} \n")
                    return True
    except json.decoder.JSONDecodeError as e:
        error(e)
        corp = set(corpus.words.words() + corpus.brown.words())
        english_vocab = set(w.lower() for w in corp)
        sample = word_tokenize(sample)
        sample_lower = set(w.lower() for w in sample if w.lower().isalpha())
        unusual = sample_lower.difference(english_vocab)
        if lenght_req:
            if len(unusual) > (len(sample) * 0.2) or len(sample) < 35:
                # print(f"Blocked: {sample_org} \n")
                return False
            else:
                # print(f"FINE: {sample_org} \n")
                return True
        else:
            if len(unusual) > (len(sample) * 0.2):
                # print(f"Blocked: {sample_org} \n")
                return False
            else:
                # print(f"FINE: {sample_org} \n")
                return True


def error(text, importance=5, *args):
    try:
        text = str(text)
        msg = str(text + f"   Time:{datetime.datetime.now()}: {[a for a in args]} IMPORTANCE:{importance}\n")
        if importance > 5:
            msg = msg.upper()
        else:
            msg = msg.lower()
        print(msg)
        try:
            with open(files["error"], "a+", encoding="UTF-8") as file:
                file.write(msg)
        except:
            print("Error could not be saved")
        try:
            with open(files["log"], "a+", encoding="UTF-8") as file:
                file.write(f"ERROR_LOG: {msg}")
        except:
            error("Could not log")
    except Exception as e:
        error(e, importance=10)


def log(text, silent=False):
    text = str(text)
    if not silent:
        print(text + f"   Time:{datetime.datetime.now()}: \n")
        try:
            with open(files["log"], "a+", encoding="UTF-8") as file:
                file.write(text + f"   Time:{datetime.datetime.now()}: \n")
        except:
            error("Could not log")
    elif silent:
        try:
            with open(files["log"], "a+", encoding="UTF-8") as file:
                file.write(text + f"   Time:{datetime.datetime.now()}: \n")
        except:
            error("Could not log")

def date_to_posix(dates, list=False, time_format="standard", timezone="+0000"):
    try:
        if list:
            posix = [time.mktime(ciso8601.parse_datetime(str(t)).timetuple()) for t in dates if type(t) is type("")]
        else:
            posix = time.mktime(ciso8601.parse_datetime(str(dates)).timetuple())
    except ValueError as e:
        try:
            if time_format == "standard":
                'Sat Apr 25 18:20:43 +0000 2020'
                time_format = "%a %b %d %X %z %Y"
            if list:
                posix = [time.mktime(datetime.datetime.strptime(str(d), time_format).timetuple()) for d in dates]
            else:
                posix = time.mktime(datetime.datetime.strptime(str(dates), time_format).timetuple())
        except Exception as e:
            error(e, importance=10)
            posix = None
    return posix

def sleep_log(time, safe_close=True):
    if safe_close:
        safe = "safe"
    else:
        safe = "not safe"
    log(f"Sleeping for {time} seconds, Its {safe} to close now! ")
    sleep(time)

def translate(text, to="en"):
    translator = Translator()
    translated = translator.translate(text, dest=to)
    return translated


def update_hdf5(file, key, dataframe, complevel=9, mode="a", append=True):
    if file == files["backup"]:
        if mode == "w":
            print("Cant delete backup. Its protected")
        if not append:
            try:
                if len(dataframe) >= len(read_hdf5(files["backup"], key)):
                    print(f"saving to {file}")
                    dataframe.to_hdf(file, key=key, mode=mode, append=append, complevel=complevel, format="table")
                else:
                    print("Cant remove data for smaller dataframe to backup. Its protected")
            except KeyError as e:
                error(e)
                print(f"saving to {file}")
                dataframe.to_hdf(file, key=key, mode=mode, append=append, complevel=complevel, format="table")

    else:
        if mode == "w":
            log(f"deleting everything in {file}, Close program within 10 sec to stop!")
            sleep(10)
        print(f"saving to {file}")
        dataframe.to_hdf(file, key=key, mode=mode, append=append, complevel=complevel, format="table")

def apply_backup(file, key):
    df = read_hdf5(files["backup"], key)
    update_hdf5(file, key, dataframe=df, append=False)
    log(f"Applied backup for {file}:{key}")

def read_hdf5(file, key):
    print(f"reading {file} for key {key}")
    try:
        data = pd.read_hdf(file, key=key)
    except FileNotFoundError:
        error("File does not exist " + str(file))
        data = False
    return data

def remove_duplicates(list):
    seen = set()
    for item in list:
        if item not in seen:
            seen.add(item)
    return seen

def del_hdf5_key(file, key):
    print(file)
    if file == files["backup"]:
        print("Cant delete backup. Its protected")
    else:
        head, tail = os.path.split(file)
        if tail in os.listdir(file.parents[0]):
            with h5py.File(file, "a") as f:
                log(f"deleted {key} in {file}")
                del f[key]

def find_tags_from_str(str_, as_str=False, known=()):
    tokens = word_tokenize(str_)
    text = str_.replace(" ","").replace("\n","")
    tags = news_tags()
    with open(files["tags_txt"], "r") as f:
        tags_ = [t.replace("\n", "").replace(" ","") for t in f.readlines()]
    [tags.add(t) for t in tags_]
    tags_space_less = [t.replace(" ","") for t in tags if type(t) is type("")]

    syms = pd.read_csv(files["all_symbols"])
    smy = [s.upper() for s in syms["Symbol"].tolist() if type(s) is type("a")]
    occ = set()
    if known:
        [occ.add(k) for k in known]
    for tag in tags_space_less:
        if tag.lower() in text.lower():
            occ.add(tag)
    for sym in smy:
        if sym in text:
            list_ = list(occ)
            for s in list_:
                if sym in str(s):
                    break
                else:
                    occ.add(sym)
    if "stock" in occ:
        if "stocks" in occ:
            occ.remove("stock")
    if as_str:
        str_ = ""
        for t in occ:
            str_ += f" {t}"
        occ = str_

    return occ


def return_tags():
    #Check when last redone
    recheck_limit = 600
    try:
        t = read_yaml(files["memory"])
        past_time = t["tags_redone"]
    except TypeError as e:
        error(e, importance=2)
        past_time = 0
        t = {}
    try:
        with open(files["tags_pickle"], "rb") as f:
            tags_old = pickle.load(f)
    except (EOFError, FileNotFoundError) as e:
        error(e)
        tags_old = {}

    if ((past_time + recheck_limit) < time.time()) or len(tags_old) == 0:
        tags_orginal = set([t for t in news_tags() if t is not None])
        with open(files["tags_txt"], "r") as f:
            tags_txt = [t.replace("\n", "") for t in f.readlines()]
        [tags_orginal.add(t) for t in tags_txt if t is not None]

        syms_ = pd.read_csv(files["all_symbols"])
        syms_short = [s.upper() for s in syms_["Symbol"].tolist() if isinstance(s, str) and len(s) <= 3]
        syms_long = [s.upper() for s in syms_["Symbol"].tolist() if isinstance(s, str) and len(s) > 3]

        tags = [t for t in tags_orginal if " " not in t]

        tags_multi_words_shortened = set()
        tags_multi_words = [t.replace("inc.", "").replace("corp.", "").replace("ltd.", "").replace("co.", "")
                                .replace("co", "").replace("  ", " ").replace("inc", "").replace(".", "").replace(",", "")
                            for t in tags_orginal if " " in t]

        short_tags = [t for t in tags if len(t) < 3]
        medium_tags = [t for t in tags if len(t) >= 3]
        long_tags = [t for t in tags if len(t) >= 10]

        for multi in tags_multi_words:
            str_multi_tag = ""
            split_multi = multi.split(" ")
            if len(split_multi) == 4:
                if split_multi[-2] != "&":
                    split_multi = split_multi[0:(len(split_multi)-1)]
            elif len(split_multi) >= 5:
                if split_multi[-3] != "&":
                    split_multi = split_multi[0:(len(split_multi)-2)]
            if split_multi[-1] == "&" or split_multi[-1] == '':
                split_multi = split_multi[0:len(split_multi)-1]
            for p in split_multi:
                if len(str_multi_tag) == 0:
                    str_multi_tag += p
                else:
                    str_multi_tag += f" {p}"
            tags_multi_words_shortened.add(str_multi_tag)
        t["tags_redone"] = time.time()
        update_yaml(files["memory"], t)
        obj = {"syms_short": syms_short, "syms_long": syms_long, "short_tags": short_tags, "medium_tags": medium_tags,
               "long_tags": long_tags, "tags_multi_words_shortened": tags_multi_words_shortened}
        with open(files["tags_pickle"], "wb") as f:
            pickle.dump(obj, f)
        return syms_short, syms_long, short_tags, medium_tags, long_tags, tags_multi_words_shortened
    else:
        return tags_old["syms_short"], tags_old["syms_long"], tags_old["short_tags"], tags_old["medium_tags"], \
               tags_old["long_tags"], tags_old["tags_multi_words_shortened"],



def tag_str(str_, as_str=False):
    if len(str_) == 0:
        return set()
    #print(str_)
    #st = time.clock()
    str_original = str_
    str_ = str_.replace("\n", "").replace("#", "").replace("@", "").replace(".", "").replace(",", "")
    space_split = str_.split(" ")
    space_split_lower = str_.lower().split(" ")
    no_space_lower = str_.lower().replace(" ", "")

    syms_short, syms_long, short_tags, medium_tags, long_tags, tags_multi_words_shortened = return_tags()

    found = set()

    for s_tag in short_tags:
        if s_tag.lower() in space_split_lower:
            found.add(s_tag)
    for sym in syms_short:
        if sym.upper() in space_split:
            found.add(sym)
    for t in medium_tags:
        if t.lower() in space_split_lower:
            found.add(t)
    for sym in syms_long:
        if sym.upper() in space_split:
            found.add(sym)

    for tag in long_tags:
        if tag.lower() in no_space_lower:
            found.add(tag)

    for multi_tag in tags_multi_words_shortened:
        if len(multi_tag) <= 6:
            if len(multi_tag) > 5:
                if multi_tag.lower() in str_original.lower():
                    found.add(multi_tag)
            else:
                if multi_tag.lower() in space_split_lower:
                    found.add(multi_tag)

        else:
            if multi_tag.replace(" ", "").lower() in no_space_lower:
                found.add(multi_tag)
    if as_str:
        print("As string")
        str_ = ""
        for t in found:
            str_ += f" {t}"
        found = str_

    #print(found, f"Took {time.clock()-st} sek to complete")
    return found



if __name__ == "__main__":
    d = read_hdf5(files["news_store"], "news_articles")
    twe = read_hdf5(files["news_store"], "twitter_conversations")
    red = read_hdf5(files["news_store"], "reddit")
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
        #from finnhub import Stream
        #Stream()
    pprint(all)
    df_tags = pd.DataFrame.from_dict(orient="index", columns=["tag"], data=all)
    df_tags.to_csv("facts/tags_gotten.csv")



