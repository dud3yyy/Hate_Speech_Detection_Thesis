import datetime

import pandas as pd
import praw

from .base_scraper import BaseScraper
from .twitter_scraper import TwitterScraper


class RedditScraper(BaseScraper):
    """Used to scrape reddit posts"""

    def __init__(self, output_path, client_id, client_secret, user_agent):
        super().__init__(output_path)
        self.reddit = praw.Reddit(
            client_id=client_id, client_secret=client_secret, user_agent=user_agent
        )

    def scrape_data(self, subreddits: list[str], query: str, limit: int):
        """Scrapes reddit posts"""
        data = []

        for subreddit in subreddits:
            sub_instance = self.reddit.subreddit(subreddit)
            for post in sub_instance.search(query=query, limit=limit):
                post.comments.replace_more(limit=0)
                for comment in post.comments.list():
                    data.append(
                        {
                            "title": post.title,
                            "body": post.selftext,
                            "author": post.author.name if post.author else None,
                            "comment": comment.body,
                            "comment_author": (
                                comment.author.name if comment.author else None
                            ),
                            "score": post.score,
                            "upvote_ratio": post.upvote_ratio,
                            "created_utc": self.convert_to_iso_format(post.created_utc),
                            "subreddit": subreddit,
                        }
                    )

        reddit_columns = data[0].keys()

        reddit_df = self.build_df(data=data, columns=reddit_columns)
        
        self.write_output(reddit_df)

    def convert_to_iso_format(self, created_utc):
        """Converts the created_utc timestamp to '%Y-%m-%d %H:%M:%S%z' format"""
        created_time = datetime.datetime.fromtimestamp(
            created_utc, tz=datetime.timezone.utc
        )

        iso_format_time = created_time.strftime("%Y-%m-%d %H:%M:%S%z")

        return iso_format_time
