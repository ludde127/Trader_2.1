from __init__ import files, check_lang_is_en, translate, error, log, read_yaml, read_hdf5, read_txt,\
    update_hdf5, del_hdf5_key, find_tags_from_str, sleep_log, date_to_posix, tag_str
from newspaper import Article, Config, news_pool, fulltext, build
from processing import to_backup
import pprint
import twitter
import tweepy
import pandas as pd
from pprint import pprint
import tables
from datetime import timezone
import datetime
import praw
import json
import pickle
import numpy as np
import yarl
import re
from time import sleep
newspaper_user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)' \
                       ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
config_newp = Config()
config_newp.browser_user_agent = newspaper_user_agent

def mine_article(url):
    art = Article(url, config=config_newp)
    art.download()
    art.parse()
    data = {}
    data["text"], data["authors"], data["publish_date"], data["url"], data["raw"] =\
        art.text, art.authors, art.publish_date,  url, art
    print("mined " + url)
    return data

def twitter_tags(limited=True, added=()):
    if not limited:
        syms = pd.read_csv(files["all_symbols"])
        all_tags = syms["Symbol"].tolist() + [w.lower() for w in syms["Name"].tolist()] + [w.lower() for w in
                                                                                           syms["Sector"].tolist()]
        all_tags = set(all_tags)
        print(all_tags)
        return all_tags
    elif limited:
        with open(files["twitter_tags"], "r") as f:
            all_tags = [t.replace("\n", "").replace(" ", "") for t in f.readlines()]
    [all_tags.append(t) for t in added]
    return all_tags

def subreddits(added=[]):
    sub = read_txt(files["subreddits"])
    sub = [tag.replace("\n", "").replace(" ", "") for tag in sub]
    sub = sub + [i for i in added]
    return sub


class TwitterNews:
    def __init__(self):
        twitter_consumer_key = read_yaml(files["auth"])["twitter_consumer_key"]
        twitter_secret_consumer_key = read_yaml(files["auth"])["twitter_secret_consumer_key"]
        twitter_access_token_key = read_yaml(files["auth"])["twitter_access_token_key"]
        twitter_secret_access_token_key = read_yaml(files["auth"])["twitter_secret_access_token_key"]

        auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_secret_consumer_key)
        auth.set_access_token(twitter_access_token_key, twitter_secret_access_token_key)
        self.ids = []
        self.store_dir = files["news_store"]

        self.api = tweepy.API(auth)

        try:
            self.api.verify_credentials()
            log("Authentication OK")

        except:
            error("Error during authentication")
            log("Error during authentication")
        try:
            data = read_hdf5(files["news_store"], key="twitter_conversations")
            ids = [int(i) for i in data.id.values]
            since_id = max(ids)
        except Exception as e:
            since_id = 1252311068950638593
            error(e)
        print(since_id)
        self.get_conversations(since_id=since_id)

    def get_conversations(self, since_id=0, tags=(), items_batch=25):

        if not tags:
            tags = [t for t in twitter_tags() if len(t) > 1]
        log(f"Getting raw data for {tags}, since_id={since_id}")
        search_str = ""
        try:
            data = read_hdf5(files["news_store"], key="twitter_conversations")
            ids = [int(i) for i in data.id.values]
            self.ids.extend(ids)
            since_id = max(ids)
        except Exception as e:
            since_id = 1252311068950638593
            error(e)
        if not since_id:
            try:
                ids = read_hdf5(self.store_dir, "twitter_conversations")["id"].values
                ids_ = True
            except (AttributeError, KeyError, TypeError) as e:
                error(str(e))
                ids_ = False
            if not ids_:
                since_id = 0
            else:
                since_id = max(ids)
        df = pd.DataFrame(columns=["created", "id", "retweets", "text", "user_id",
                                   "favorits", "user_followers"])
        tags = [tag for tag in tags if type(tag) is type(str())]


        for t in tags[:40]:
            search_str += f"{t} OR "
        if search_str[-3:] == "OR ":
            search_str = search_str[:-4]
        search_str += " -filter:retweets"
        print(search_str)

        for i in range(5):
            try:
                since_id = max(self.ids)
                if since_id:
                    cursor = tweepy.Cursor(self.api.search, q=search_str, lang="en", full_text=True, since_id=since_id,
                                           wait_on_rate_limit=True, tweet_mode="extended",
                                           wait_on_rate_limit_notify=True, retry_delay=5).items(items_batch)
                else:
                    cursor = tweepy.Cursor(self.api.search, q=search_str, lang="en", full_text=True,
                                           wait_on_rate_limit=True, tweet_mode="extended",
                                           wait_on_rate_limit_notify=True, retry_delay=5).items(items_batch)
                for tweet in cursor:
                    j = tweet._json
                    if j["id"] in self.ids:
                        print(str(j["id"]) + " already in wtf")
                    else:
                        created = date_to_posix(str(j["created_at"]), list=False)

                        if created is not None:
                            #print(j["created_at"])
                            #date = datetime.datetime.strptime(j["created_at"], "%a %b %H %M %S %z %-y").replace(tzinfo=timezone.utc).timestamp()
                            #print(date)

                            data = {"created": float(created), "id": str(j["id"]), "retweets": str(j["retweet_count"]),
                                    "text": str(j["full_text"]), "user_id": str(j["user"]["id"]), "favorits": str(j["favorite_count"]),
                                    "user_followers": str(j["user"]["followers_count"])}
                            #tag_str(j["full_text"], as_str=True)

                            self.ids.append(int(j["id"]))
                            if len(data["text"]) >= 333:
                                print(data["text"], "left out")
                            else:
                                if len(data["text"]) > 25:
                                    df = df.append(data, ignore_index=True)
                        else:
                            print(j)

                df.set_index("created", inplace=True)
                print(df)

                self.ids.extend([int(v) for v in df.id.values])
                update_hdf5(files["news_store"], key="twitter_conversations", dataframe=df, append=True)
                df = pd.DataFrame(columns=["created", "id", "retweets", "text", "user_id",
                                           "favorits", "user_followers"])

            except FileNotFoundError as e:
                error(str(e))

            #df.set_index("created_date", inplace=True)
            #df = df.join(pd.read_hdf(pd.read_hdf(files["news_store"], key="twitter_conversations")))


class Reddit:
    def __init__(self):
        #del_hdf5_key(files["news_store"], "reddit")

        try:
            if len(read_hdf5(files["news_store"], "reddit")) <= 1:
                error("Too short new")
                self.df_subs = pd.DataFrame(columns=["id", "text", "title", "created", "score", "comments", "comment_num"])
            else:
                self.old_save = True
                self.df_subs = read_hdf5(files["news_store"], "reddit")
        except KeyError as e:
            error(e)
            self.df_subs = pd.DataFrame(columns=["id", "text", "title", "created", "score", "comments", "comment_num"])
        try:
            self.ids = list(read_hdf5(files["news_store"], "reddit").id.values)
        except KeyError as e:
            error(e)
            self.ids = list()
        user_agent = "Windows: News Analyser :v1.0 (by /u/ludvig127)"
        self.red = praw.Reddit(user_agent=user_agent, client_id=read_yaml(files["auth"])["reddit_client_id"],
                               client_secret=read_yaml(files["auth"])["reddit_client_secret"])

    def scrape_subreddit(self, added_subreddits=(), items=10, return_items=False, only_added=False):
        if only_added:
            subreddits_ = added_subreddits
        else:
            subreddits_ = subreddits(added=added_subreddits)
        if items is None:
            items = 50
        subreddits_gotten = []
        for subreddit in subreddits_:
            hot_posts = self.red.subreddit(subreddit).hot(limit=items)
            subreddits_gotten.append((hot_posts, subreddit))
        comments = {}
        for sub in subreddits_gotten:
            comments[sub] = self.parse_submission_obj(sub)
        try:
            with open(files["reddit_comments"], "rb") as f:
                old = pickle.load(f)
        except (FileNotFoundError, EOFError) as e:
            old = {}
            error(e)
        with open(files["reddit_comments"], "wb") as f:
            if old is not None:
                pickle.dump(comments.update(old), f)
            else:
                pickle.dump(comments, f)
        print(self.df_subs)
        update_hdf5(files["news_store"], "reddit", dataframe=self.df_subs, mode="a", append=False)

        if return_items:
            return self.df_subs, comments

    def parse_submission_obj(self, obj):
        c = 0
        comment_dict = {}
        for item in obj[0]:
            c += 1
            try:
                eng = check_lang_is_en(item.title)
            except ValueError as e:
                print(str(e))
                eng = check_lang_is_en(item.title, safe=True, lenght_req=False)

            if eng:
                title = item.title
                id = item.id
                print(title)
                total_comment_score = [0, 0]
                comment_list = []
                comment = []
                for com in item.comments:
                    dict_top = dict()
                    try:
                        dict_top["created"], dict_top["score"], dict_top["body"], dict_top["id"], dict_top["parent_id"], \
                        dict_top["top_comment"] = \
                            com.created_utc, com.score, com.body, com.id, com.parent_id, True
                    except AttributeError as e:
                        error(e)
                        continue
                    total_comment_score[0] += abs(com.score)
                    total_comment_score[1] += com.score
                    comment.append(dict_top)

                    for repl in com.replies:
                        dict_top = dict()
                        try:
                            dict_top["created"], dict_top["score"], dict_top["body"], dict_top["id"], dict_top["parent_id"], \
                            dict_top["top_comment"] = \
                                repl.created_utc, repl.score, repl.body, repl.id, repl.parent_id, False
                        except AttributeError as e:
                            if "MoreComments" in e:
                                pass
                            else:
                                error(str(e))
                        comment.append(dict_top)
                        total_comment_score[0] += abs(repl.score)
                        total_comment_score[1] += repl.score
                    comment_list.append(comment)
                data = {"id": str(id), "text": str(item.selftext.replace("\n", "")), "title": str(title),
                        "created": float(item.created_utc),
                        "comment_num": str(item.num_comments), "sub": str(item.subreddit.name),
                        "score": f"{str(item.score)}:{str(item.upvote_ratio)}"}
                if id in self.ids:
                    self.df_subs = self.df_subs[self.df_subs.id != id]
                    self.df_subs = self.df_subs.append(data, ignore_index=True)
                else:
                    self.df_subs = self.df_subs.append(data, ignore_index=True)
                self.ids.append(id)
                comment_dict[item.id] = comment_list
        return comment_dict

                #"comments": pickle.dumps(comment_list) "score": f"{str(item.score)}:{str(item.upvote_ratio)},

class NewsArticles:
    def __init__(self):
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
                     "(KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36 OPR/66.0.3515.44"
        self.config = Config()
        self.config.browser_user_agent = user_agent
        self.config.memoize_articles = True
        self.config.verbose = True
        self.config.language = "en"

        ignore_already_gotten = True
        #nono_host_words = ["arabic", "espanol", "latino", "latina"]
        self.df_art_base = pd.DataFrame(
            columns=["link", "text", "title", "created", "keywords", "author"])
        try:
            self.newssites = (["https://www.cnbc.com", 'http://cnn.com', "http://www.huffingtonpost.com",
                               "http://www.nytimes.com", "http://news.bbc.co.uk/", "http://www.theguardian.com/"])

        except Exception as e:
            error(str(e))
        self.urls = {}
        try:
            if len(read_hdf5(files["news_store"], "news_articles")) <= 1:
                error("Too short new")
                del_hdf5_key(["news_store"], "news_articles")
                self.df_art = pd.DataFrame(
                    columns=["link", "text", "title", "created", "keywords", "author"])
            else:
                self.df_art = read_hdf5(files["news_store"], "news_articles")
        except KeyError as e:
            error(e)
            self.df_art = pd.DataFrame(
                columns=["link", "text", "title", "created", "keywords", "author"])
        except TypeError:
            error("Too short new")
            del_hdf5_key(files["news_store"], "news_articles")
            self.df_art = pd.DataFrame(
                columns=["link", "text", "title", "created", "keywords", "author"])
        try:
            self.urls["gotten"] = read_hdf5(files["news_store"], "news_articles").link.values
        except (KeyError, AttributeError) as e:
            error(e)
            self.urls["gotten"] = []

    def parse_article(self, url, save_to_self=True):
        art = Article(url)
        art.download()
        art.parse()
        dic_temp = {"link": str(art.url), "text": str(art.text), "title": str(art.title),
                    "created": str(art.publish_date), "keywords": str(art.keywords), "author": str(art.authors)}
        df = self.df_art_base.append(dic_temp, ignore_index=True)
        if art.url not in self.urls["gotten"]:
            update_hdf5(files["news_store"], "news_articles", dataframe=df)
        if save_to_self:
            self.df_art = self.df_art.append(dic_temp, ignore_index=True)
        else:
            return dic_temp

    def gather_different(self, extra_urls=None, only_extra=False, ignore_gotten=False, save=True):
        checklang = False
        if extra_urls:
            self.urls["extras"] = set(extra_urls)
            for url_ext in extra_urls:
                mine_article(url_ext)
        if not only_extra:
            print(self.newssites)
            if len(self.newssites) > 1 and type(self.newssites) is list:
                papers = [build(paper, config=self.config) for paper in self.newssites]
            else:
                papers = build(self.newssites, config=self.config)
            log(f"Getting Data from {len(self.newssites)} newssites...")
            news_pool.set(papers, threads_per_source=2)
            news_pool.join()
            for art_pool, url in zip(papers, self.newssites):
                print(f"Handling newssite {int(self.newssites.index(url)) + 1}/{len(self.newssites)}")
                for art in art_pool.articles:
                    art.parse()
                    if (str(art.url) not in self.urls["gotten"]) or ignore_gotten:
                        created = date_to_posix(dates=art.publish_date, list=False)
                        if created is not None and created != "None":
                            dic_temp = {"link": str(art.url), "text": str(art.text.replace("  ", "").replace("\n", "")),
                                        "title": str(art.title),
                                        "created": float(created), "keywords": str(art.keywords),
                                        "author": str(art.authors)}
                            self.urls["gotten"] = np.append(self.urls["gotten"], art.url)
                            if checklang:
                                try:
                                    if check_lang_is_en(str(art.text)):
                                        self.df_art = self.df_art.append(dic_temp, ignore_index=True)
                                    else:
                                        print(f"Blocked: {dic_temp['text']}")
                                except json.decoder.JSONDecodeError as e:
                                    error(e)
                                    if check_lang_is_en(str(art.title)):
                                        self.df_art = self.df_art.append(dic_temp, ignore_index=True)
                                    else:
                                        print(f"Blocked: {dic_temp['text']}")
                                    print("fixed?")
                            else:
                                self.df_art = self.df_art.append(dic_temp, ignore_index=True)

        if save:
            print(self.df_art)
            try:
                pass
                #print(self.df_art.to_string())
            except:
                pass
            update_hdf5(files["news_store"], "news_articles", dataframe=self.df_art, mode="a", append=False)


#df, coms = Reddit.scrape_subreddit(Reddit(), return_items=True, items=5)
#print(df)
#d = [int(i) for i in pd.read_hdf(files["news_store"], key="twitter_conversations").favorits.values if int(i) != 0]
#print(d)

#print(d)
#update_hdf5("news_data_new.h5", "news_articles", dataframe=d, append=False)

#print(read_hdf5(["news_store"], "twitter_conversations"))

if __name__ == "__main__":
    loop = True
    runs = 0
    while loop:

        #print(read_hdf5(files["news_store"], "twitter_conversations").columns)

        #update_hdf5(files["news_store"], "twitter_conversations", read_hdf5(files["news_store"], "twitter_conversations").drop("tag", inplace=False, axis=1), append=False)
        #update_hdf5(files["news_store"], "twitter_conversations", read_hdf5(files["news_store"], "twitter_conversations").set_index("created", inplace=False), append=False)

        print(read_hdf5(files["news_store"], "twitter_conversations"))
        runs += 1
        TwitterNews()
        #print(read_hdf5(files["news_store"], "twitter_conversations").tag)

        try:
            pass
            Reddit.scrape_subreddit(Reddit())
        except Exception as e:
            error(e)
            pass
        #del_hdf5_key(files["news_store"], "news_articles")
        NewsArticles.gather_different(NewsArticles())
        print(read_hdf5(files["news_store"], "twitter_conversations"))
        print(read_hdf5(files["news_store"], "news_articles"))
        print(read_hdf5(files["news_store"], "reddit"))
        if runs == 200:
            log("Doing backup")
            to_backup(files["news_store"], "twitter_conversations", append=False)
            to_backup(files["news_store"], "news_articles", append=False)
            to_backup(files["news_store"], "reddit", append=False)
            runs = 0
        sleep_log(30)

