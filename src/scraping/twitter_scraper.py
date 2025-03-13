from typing import Any

import pandas as pd
import tweepy

from .base_scraper import BaseScraper


class TwitterScraper(BaseScraper):
    """Twitter scraping class which inherits the base scraper class"""

    def __init__(self, bearer_token: str, output_path: str):
        super().__init__(output_path)
        self.client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

    def scrape_data(self, query: str, count: int) -> None:
        """
        Method that scrapes and ingests recent tweets

        Args:
            query (str): Represents the query which will be used to query the data.
            count (int): Represents the number of tweets we want to pull.
        """
        # TODO
        # Check whether the csv file is open and cancel the run

        response = self.get_recent_tweets(query, count)
        if response.data:
            attributes_cont = self.map_data(response)

            columns = [
                "Username",
                "Country_Code",
                "Possibly_Sensitive",
                "Date_Created",
                "No_of_Likes",
                "Source",
                "Full_Text",
            ]

            x_df = self.build_df(columns=columns, data=attributes_cont)

            self.write_output(x_df)
            self.logger.info(
                f"Successfully saved {len(x_df)} tweets to {self.output_path}"
            )
        else:
            self.logger.info("No tweets found for this query!!!")

    def get_recent_tweets(self, query: str, count: int) -> tweepy.Response:
        """
        Retrieves recent tweets based on the specified query and count.

        Args:
            query (str): Represents the query which will be used to query the data.
            count (int): Represents the number of tweets we want to pull.

        Returns:
            tweepy.Response: A response object that contains the tweets.
        """
        self.logger.info(f"Scraping recent tweets based on this query: {query}")
        return self.client.search_recent_tweets(
            query=query,
            tweet_fields=[
                "author_id",
                "geo",
                "possibly_sensitive",
                "created_at",
                "public_metrics",
                "text",
                "source",
            ],
            user_fields=["username", "location"],
            expansions="author_id",
            max_results=count,
        )

    def map_data(self, response: Any) -> list:
        """
        Maps the data accordingly

        Args:
            response (Any): Represents the response w the scraped data

        Returns:
            list: contains the necessary mappings
        """
        users = {user["id"]: user for user in response.includes["users"]}
        self.logger.info("Mapping the data...")
        return [
            [
                (
                    users[tweet.author_id]["username"]
                    if tweet.author_id in users
                    else None
                ),
                tweet.geo["place_id"] if tweet.geo else None,
                tweet.possibly_sensitive,
                tweet.created_at,
                tweet.public_metrics["like_count"],
                tweet.source if tweet.source else "X",
                tweet.text,
            ]
            for tweet in response.data
        ]
