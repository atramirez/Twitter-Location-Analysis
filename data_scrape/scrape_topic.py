"""
Author: atramirez

Usage: Run this script as __main__
It will prompt you with the hashtag, amount of tweets to get, and your name
It will scrape and place your file in /data/init_scrape
"""
# Python Imports
import datetime
import pathlib

# External imports
import snscrape.modules.twitter as sntwitter
import pandas as pd


"""
Scrape function
"""
def scrape_x_tweets(hashtag, scrape_num):

    # Creating list to append tweet data to
    attributes_container = []

    # Using TwitterSearchScraper to scrape data and append tweets to list
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(f'{hashtag} since:{datetime.date.today()} until:{datetime.date.today() + datetime.timedelta(days=1)}').get_items()):
        if i>scrape_num:
            break
        attributes_container.append([tweet.user.username, tweet.date, tweet.coordinates, tweet.sourceLabel, tweet.content])
    return attributes_container

def write_df_to_csv(name:str, attributes_container:list, hashtag:str):
    # Creating a dataframe to load the list
    tweets_df = pd.DataFrame(attributes_container, columns=["User", "Date Created", "GeoJSON", "Source of Tweet", "Tweet"])

    # Get time for file name
    now = datetime.datetime.now()
    current_time = now.strftime("%H-%M-%S")

    # make csv path
    csv_path = pathlib.Path(__file__).resolve()
    csv_path = csv_path.parent
    csv_path = csv_path.parent
    csv_path = pathlib.Path(csv_path, f"data/init_scrape/{name}_{hashtag}_{datetime.date.today()}_{current_time}.csv")
    tweets_df.to_csv(pathlib.Path(csv_path))


if __name__ == "__main__":
    print("================================================")
    print("                    Input                        ")
    print("================================================")
    hashtag_prompt = input("Hashtag: ")
    scrape_num = input("Tweet Number: ")
    name = input("Name: ")
    
    print("================================================")
    print("                   Scraping                     ")
    print("================================================")
    atrr_cont = scrape_x_tweets(hashtag=hashtag_prompt, scrape_num=int(scrape_num))
    print("Finished Scrape")  
    
    print("================================================")
    print("                    Output                      ")
    print("================================================")
    print("Attempting to Write to .CSV File")
    write_df_to_csv(name=name, attributes_container=atrr_cont, hashtag=hashtag_prompt)
