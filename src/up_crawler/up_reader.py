import pdb
import sys
import traceback
import argparse

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

import re

from pathlib import Path

import requests
from bs4 import BeautifulSoup
from unicodedata import normalize

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
    retry_if_not_exception_message,
    retry_if_not_exception_type,
    retry_if_exception_type,
)

import dateparser
from datetime import datetime

import pandas as pd

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from rich import print
from rich import inspect

import base64

import csv

from typing import List, Tuple, Optional, Dict, Union, Iterator

from collections import defaultdict

from up_crawler.randomization import RandomizationParams, _parse_timeout
from up_crawler.path_ops import get_dir_or_temp, mkdir, make_path_ok, get_file_or_temp
from up_crawler.get_uris import UPSitemapCrawler

from up_crawler.data_structures import Language, Article, TagsMapping, FullArticle
from up_crawler.consts import URI_TAGS_RU, URI_TAGS_UA, REGEX_PARAS_TO_SKIP
from up_crawler.consts import MAX_RETRIES_FOR_REQUEST, TAGS_MAPPING_FN


b = breakpoint


class UPReader:
    """
    Reads the crawl results of UParser.
    """

    # Paragraphs matching either of those will be skipped

    def __init__(
        self,
        input_dir: Path | str,
        target_dir: Optional[Path | str] = None,
        tags_mapping_file: Optional[Path] = None,
    ):
        self.input_dir = make_path_ok(input_dir)

        self.target_dir: Path = get_dir_or_temp(target_dir)

        if (
            tags_mapping_file
            and Path(tags_mapping_file).exists()
            and Path(tags_mapping_file).is_file()
        ):
            # If we have a working one, use it
            self.tags_mapping_file = make_path_ok(tags_mapping_file)
        else:
            if not tags_mapping_file:
                # If we didn't get one, create one in the target dir
                self.tags_mapping_file = self.input_dir / TAGS_MAPPING_FN
            else:
                raise ValueError(f"No tags mapping file found or provided")

    def read(self):
        """Read input dir, return dict of art_id->three versions of the same article."""
        arts = self.read_dir(path=self.input_dir)
        tags = self.get_tags(tags_file=self.tags_mapping_file)
        return arts

    @staticmethod
    def get_tags(tags_file: Path) -> TagsMapping:
        try:
            tm = TagsMapping.from_json_file(tags_file)
        except Exception as e:
            logger.exception(e)
            raise e
        return tm

    @staticmethod
    def is_dir_articles_dir(path: Path) -> bool:
        """Checks if path is valid dir with N articles."""
        if not path.is_dir():
            return False

        def is_correct_fn(fn: str) -> bool:
            if fn.split(".")[-1] != "json":
                return False
            lang = fn.split("_")[0]
            if lang not in [x.value for x in Language]:
                return False
            return True

        art_files = [x for x in list(path.iterdir()) if is_correct_fn(x.name)]
        if not art_files:
            return False
        return True

    @staticmethod
    def read_article_dir(d: Path) -> FullArticle:
        """Read individual artilce in dir"""

        all_articles = defaultdict(dict)
        fa_tags = set()
        last_date_published = None
        for art_file in d.iterdir():
            if art_file.suffix != ".json":
                continue
            try:
                article = Article.from_json_file(art_file)
                art_id = int(d.name)
                lang = art_file.name.split("_")[0]
                # NB - same article in diff languages can have diff tags!
                #   e.g. see 7430996
                fa_tags = fa_tags.union(article.tags)
                last_date_published = article.date
            except Exception as e:
                logger.warning(f"Failed to read {art_file} as article: {e}")
                continue
            all_articles[lang] = article
        fa = FullArticle(
            articles=all_articles,
            art_id=art_id,
            date_published=last_date_published,
            tags=list(fa_tags),
        )

        return fa

    @staticmethod
    def read_dir(path: Path) -> list[FullArticle]:
        """Read all articles in path, return as list of FullArticles."""
        all_fas = list()
        dirs = path.iterdir()

        num_files = len(list(path.iterdir()))

        for d in tqdm(dirs, total=num_files):
            if not UPReader.is_dir_articles_dir(d):
                continue
            fa = UPReader.read_article_dir(d)
            all_fas.append(fa)
        if not all_fas:
            raise ValueError(f"No valid articles found in {path}")
        return all_fas

    @staticmethod
    def read_dir_chunked(path: Path) -> list[FullArticle]:
        """Read all articles in path, return as list of FullArticles."""
        dirs = path.iterdir()

        for d in dirs:
            if not UPReader.is_dir_articles_dir(d):
                continue
            fa = UPReader.read_article_dir(d)
            yield fa


class UPToCSVExporter:
    """Class that generates .csv files from the list of
    FullArticles generated by UPReader."""

    # TODO https://www.pravda.com.ua/news/2023/08/10/7414962/ why few tags?

    FA_FIELDS = ['art_id', 'date_published', 'tags']
    FIELDS = ["uri", "title", "author_name", "text", "tags", "tags_full"]

    def fa_to_row(fa: FullArticle, fields=FIELDS) -> dict:
        row = dict()
        row["art_id"] = fa.art_id
        row["date_published"] = fa.date_published
        row["tags"] = ','.join(fa.tags)
        #  row["articles"] = fa.articles

        #  ex_langs = [x.value for x in Language]
        ex_langs = [x for x in Language]
        for l in ex_langs:
            art = fa.articles.get(l, None)
            for f in fields:
                fn = f"{l}_{f}"
                if art:
                    fv = art.__getattribute__(f)
                else:
                    fv = None

                if fv is None:
                    fv_clean = fv
                elif f == "tags":
                    #  fv_clean = ",".join(fv)
                    fv_clean = ','.join([x[1] for x in art.tags_full])
                elif f == "text":
                    fv_clean = fa.articles[l].get_text()
                else:
                    fv_clean = fv
                row[fn] = fv_clean
        return row

    def fas_to_csv(
        fas: list[FullArticle] | Iterator[FullArticle], target_csv: Path
    ) -> None:
        logger.info(f"Writing to CSV {str(target_csv)}")
        fieldnames = list()
        fieldnames.extend(UPToCSVExporter.FA_FIELDS)
        for l in Language:
            for fn in UPToCSVExporter.FIELDS:
                fieldnames.append(f"{l}_{fn}")

        with open(target_csv, "w", newline="") as csvfile:
            pwriter = csv.DictWriter(
                #  csvfile, fieldnames=fieldnames, delimiter=" ", quotechar="|", quoting=csv.QUOTE_MINIMAL
                csvfile, fieldnames=fieldnames, dialect="unix", 
            )
            pwriter.writeheader()
            # TODO tqdm?
            for i,fa in enumerate(fas):
                #  first = fas[0] if isinstance(fas, list) else next(fas)
                row = UPToCSVExporter.fa_to_row(fa)
                pwriter.writerow(row)
                if i%50==0:
                    logger.info(f"{i} lines written")
            logger.info(f"Finished writing {i} articles to {str(target_csv)} ")
        pass


def run(args):
    logger.info(f"Running with params {args}")

    ur = UPReader(input_dir=args.input)
    #  ur.read()
    chunks = ur.read_dir_chunked(args.input)
    target_file = get_file_or_temp(Path(args.output))
    r = UPToCSVExporter.fas_to_csv(fas=chunks, target_csv=target_file)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        "-i",
        help="Input file with the file containing article URIs to crawl",
        type=Path,
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output for the dataset (%(default)s)",
        type=Path,
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
            logger.exception(e)


if __name__ == "__main__":
    main()


######
# TODO:
#   - support not just /news/ but also /articles/, /columns/ etc.
#   - "що передувало": https://www.pravda.com.ua/news/2023/10/10/7423534/
#       - or just remove the text itself if I won't be implementing that
#       - generally, look into all article texts that end up in ":"
#  lines written to CSV#   - "I'm running, and I haven't crawled stuff for 5 days, I'll go through the 5 days" or something
