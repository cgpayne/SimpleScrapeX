import snscrape.modules.twitter as sntwitter
import pandas as pd
from datetime import datetime

def scrape_query(query, max_results=10):
    tweets = []
    for ii, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
        if ii >= max_results:
            break
        tweets.append({
            "date": tweet.date.isoformat(),
            "user": tweet.user.username,
            "content": tweet.content,
            "url": tweet.url,
        })
    return tweets

def scrape_queries(queries, max_results=10):
    all_results = []
    for query in queries:
        result = scrape_query(query, max_results=max_results)
        for tweet in result:
            tweet["query"] = query  # tag source query
        all_results.extend(result)
    return pd.DataFrame(all_results)

def save_to_parquet(df, filename=None):
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tweets_{timestamp}.parquet"
    df.to_parquet(filename, index=False)
    return filename
