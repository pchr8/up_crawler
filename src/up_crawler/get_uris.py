"""
This file contains the logic to parse the sitemap and 
get URIs of articles published on those days.
"""

import pdb
import sys
import traceback
import argparse

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

from pathlib import Path

import re

import requests
from bs4 import BeautifulSoup
from unicodedata import normalize
from urllib.parse import urlparse
from urllib.error import HTTPError

import advertools as adv

import dateparser
from datetime import datetime

import pandas as pd

from dataclasses import dataclass

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from typing import List, Tuple, Optional, Dict, Union

from up_crawler.data_structures import (
    Language,
    Article,
)

from up_crawler.randomization import RandomizationParams

from up_crawler.consts import URI_REGEX_EXT
from up_crawler.path_ops import (
    make_path_ok,
    make_writable,
    get_dir_or_temp,
    get_file_or_temp,
)

b = breakpoint

# If true will sometimes do breakpoints, disable in 'prod'


class UPSitemapCrawler:
    """Parses sitemap and gets URIs of articles to later download."""

    # Latest (~1month-ish?) articles - TODO implement
    SITEMAP_CURRENT_MONTH_URI = "https://www.pravda.com.ua/sitemap/sitemap-news.xml"

    # Articles from before that
    SITEMAP_MONTH_ARCHIVE_URI = (
        "https://www.pravda.com.ua/sitemap/sitemap-{year}-{month:02d}.xml.gz"
    )

    """
    https://www.pravda.com.ua/sitemap/sitemap.xml
        https://www.pravda.com.ua/sitemap/sitemap-archive.xml
            https://www.pravda.com.ua/sitemap/sitemap-2023-04.xml.gz
        https://www.pravda.com.ua/sitemap/sitemap-news.xml
        https://www.pravda.com.ua/sitemap/sitemap-now.xml
    """

    @classmethod
    def _get_sitemap_uri_for_month(cls, day: datetime):
        # TODO - error out if we're in the future
        # TODO - handle archive VS news sitemaps
        if day.month > datetime.now().month:
            logger.error(f"{day} is in the future, no sitemap!")
            return None

        year = day.year
        month = day.month

        sitemap_uri = cls.SITEMAP_MONTH_ARCHIVE_URI.format(year=year, month=month)
        return sitemap_uri

    def get_articles_from_sitemap(self, sitemap_uri: str) -> Optional[pd.DataFrame]:
        """Get articles from sitemap at URI, return DataFrame with columns
        domain, lang, art_id.

        If file not found return None, raises all other HTTP/connection exceptions
        """
        # we expect to get an archive sitemap, so no cool metadata from news sitemap
        # we emphatically don't trust lastmod because it's not publishing date
        try:
            dfo = adv.sitemap_to_df(sitemap_uri, recursive=False)
        except HTTPError as e:
            if e.status == 404:
                logger.debug(f"No sitemap at {sitemap_uri}")
                return None
            logger.error(f"HTTPError when getting sitemap {sitemap_uri}")
            logger.exception(e)
            raise

        # dataframe with capture groups extracted as columns
        # we expect all URIs to have a trailing slash!
        df = dfo["loc"].str.extract(URI_REGEX_EXT)
        df["date"] = pd.to_datetime(df.date_part, format="%Y/%m/%d")

        # ukrainian language where not mentioned in the URI, so ukr/rus/eng
        df.loc[df.lang.isna(), "lang"] = Language.UA.value
        df = df[["uri", "date", "domain", "lang", "kind", "art_id", "id"]]
        df = df[df.kind == "news"]
        return df

    @staticmethod
    def _filter_arts_by_hr_date(
        df: pd.DataFrame,
        d1: Union[datetime, str],
        d2: Optional[Union[datetime, str]] = "yesterday",
    ) -> Dict[str, list[str]]:
        """Given two datetimes, filter the dataframe by dates in that range.

        - Fuzzy bits like "last month" possible in params
        - d2 empty means "yesterday" inclusive
        """

        logger.debug(f"Getting links from {d1} to {d2}")
        d1p = dateparser.parse(d1) if isinstance(d1, str) else d1
        d2p = dateparser.parse(d2) if isinstance(d2, str) else d2
        filtered = df[(d1p < df.date) & (df.date < d2p)]
        return filtered

    def save_articles_df(self, df, save_path: Optional[Path | str] = None) -> Path:
        def build_fn():
            start_date = str(df.sort_values("date").date.iloc[0].date())
            end_date = str(df.sort_values("date", ascending=False).date.iloc[0].date())
            fn = f"uris_list_{start_date}-{end_date}_{len(df)}.csv"
            return fn

        # filename just in case
        fn = build_fn()

        # Find some place to save to, based on whether save_path is a file or directory
        path = get_file_or_temp(save_path, fn_if_needed=build_fn())
        df.to_csv(path, index=False)
        logger.info(f"Saved df to {str(path)}")

        return path

    def get_article_uris(
        self,
        d1: Union[datetime, str],
        d2: Optional[Union[datetime, str]] = "yesterday",
    ) -> pd.DataFrame:
        """Get DataFrame with parsed article URIs for articles
        between d1 and d2 dates (plaintext like 'last year' works!)

        If d2 is not provided, "yesterday" is assumed.

        Uses UPravda's archive sitemap, which doesn't have the most
        recent articles (~1 months old and newer).

        Args:
            d1 (Union[datetime, str]): d1
            d2 (Optional[Union[datetime, str]]): d2

        Returns:
            pd.DataFrame: dataframe with articles and semantically meaningful columns
        """
        # TODO use 'news' sitemap for the most recent articles not found in archive!
        d1p = dateparser.parse(d1) if isinstance(d1, str) else d1
        d2p = dateparser.parse(d2) if isinstance(d2, str) else d2
        logger.info(
            f"Getting URLs of articles published between {d1p.date()} ('{d1}') and {d2p.date()} ('{d2}')"
        )

        if d1p.date() == d2p.date():
            raise ValueError(f"Dates should differ!")

        # Add a ~month so that the date range will definitely include d2
        # TODO draw this to see whether teh concept is sound
        d2p_safe = d2p + pd.to_timedelta(30, "days")

        # Range of months
        months_range = pd.date_range(start=d1p, end=d2p_safe, freq="M")
        logger.debug(f"{months_range=}")

        all_arts = list()
        for m in months_range:
            sm_uri = self._get_sitemap_uri_for_month(m)
            logger.debug(f"{m} -> {sm_uri}")
            # if we ended up in the future that's okay, skip
            if sm_uri is None:
                continue
            df_articles = self.get_articles_from_sitemap(sm_uri)
            if df_articles is None:
                continue
            all_arts.append(df_articles)
        df_full = pd.concat(all_arts)
        df_filt = self._filter_arts_by_hr_date(df_full, d1p, d2p)
        df_filt = df_filt.sort_values("date")

        if not len(df_filt):
            msg = f"No articles found matching the criteria!"
            if d2p > dateparser.parse("one month ago"):
                msg += " Only articles present in archive page are currently"
                " downloadable (~10 days ago+), try older articles!"
            raise ValueError(msg)
        logger.info(f"Got {len(df_filt)} article URLs!")

        return df_filt

    def get_and_save_article_uris(
        self,
        d1: Union[datetime, str],
        d2: Optional[Union[datetime, str]] = "yesterday",
        save_path: Optional[str | Path] = None,
    ) -> Path:
        """Get the URIs of articles published between dates
        d1 and d2, get them into into a dataframe  with parsed
        URI parts as columns, and save to disk.

        Paths can be either a file (used as-is) or a directory (will
        generate a filename and write it there) or None (will use a temp dir)

        Dates can be datetimes or human ones ('three days ago').

        Args:
            d1 (Union[datetime, str]): datetime or 'last year'
            d2 (Optional[Union[datetime, str]]): same; None means 'yesterday'
            save_path (Optional[str|Path]): save_path

        Returns:
            path where the DF was saved
        """

        df = self.get_article_uris(d1=d1, d2=d2)
        res = self.save_articles_df(df, save_path=save_path)
        return res


def run(args):
    date_1 = args.date_start  # if d1 else "3 days ago"
    date_2 = args.date_end  # if d2 else 'yesterday'
    output_path = args.output

    uc = UPSitemapCrawler()
    res = uc.get_and_save_article_uris(d1=date_1, d2=date_2, save_path=output_path)
    #  print(res)


def parse_args() -> argparse.Namespace:
    DEFAULT_START_DATE = "three days ago"
    DEFAULT_END_DATE = "yesterday"

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output",
        "-o",
        help="Output file or dir for the list of article URIs. If not provided a tempdir will be used. (%(default)s)",
        type=Path,
        required=False,
    )
    parser.add_argument(
        "--date_start",
        "-ds",
        help="Starting date for articles to be parsed, as str (%(default)s)",
        type=str,
        default=DEFAULT_START_DATE,
    )
    parser.add_argument(
        "--date_end",
        "-de",
        help="End data for articles, empty=='yesterday' (%(default)s)",
        type=str,
        default=DEFAULT_END_DATE,
    )
    parser.add_argument("--pdb", "-P", help="Run PDB on exception", action="store_true")
    parser.add_argument(
        "-q",
        help="Output only warnings",
        action="store_const",
        dest="loglevel",
        const=logging.WARN,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Output more details",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger.setLevel(args.loglevel if args.loglevel else logging.INFO)

    logger.debug(args)

    try:
        run(args)
    except Exception as e:
        if args.pdb:
            extype, value, tb = sys.exc_info()
            traceback.print_exc()
            pdb.post_mortem(tb)
        else:
            raise e


if __name__ == "__main__":
    main()


######
# TODO:
#   - support not just /news/ but also /articles/, /columns/ etc.
#   - "що передувало": https://www.pravda.com.ua/news/2023/10/10/7423534/
#       - or just remove the text itself if I won't be implementing that
#       - generally, look into all article texts that end up in ":"
#   - "I'm running, and I haven't crawled stuff for 5 days, I'll go through the 5 days" or something

#  c.arts[0].arts[Language.RU].text
