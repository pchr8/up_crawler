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
from requests import ConnectionError, ReadTimeout
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

import base64

from typing import List, Tuple, Optional, Dict, Union

from up_crawler.data_structures import (
    Language,
    Article,
    TagsMapping,
)

from up_crawler.randomization import RandomizationParams, _parse_timeout
from up_crawler.consts import URI_TAGS_RU, URI_TAGS_UA, REGEX_PARAS_TO_SKIP

from up_crawler.consts import MAX_RETRIES_FOR_REQUEST, TAGS_MAPPING_FN

from up_crawler.path_ops import get_dir_or_temp, mkdir, get_file_or_temp, make_path_ok

from up_crawler.get_uris import UPSitemapCrawler

b = breakpoint


class UPCrawler:
    """
    Downloads articles from Ukrainska Pravda (https://www.pravda.com.ua/).
    Downloads articles in Ukrainian, as well as Russian and English if present.

    Uses UPSitemapCrawler's dataframes (with URIs and metadata) as input for download.

    TODO feature download only articles in one language?
    TODO log to file with loglevel DEBUG and to screen with INFO
    TODO rewrite to use newspaper3k? if possible, it's much more future proof than hardcoded
    TODO skip paras that ask to support UP, become patron, follow on TG and whatever

    TODO next candidate website is https://www.epravda.com.ua/, has ukr+rus
    """

    # Paragraphs matching either of those will be skipped

    # Tags that contain text paragraphs (not adverts etc.) in an article text
    # e.g. https://www.pravda.com.ua/eng/news/2023/09/1/7418042/ has them:
    #   p (as always), and ul-> li (bullet point list around 'background')
    PARAS_WITH_TEXT = ["p", "li"]

    def __init__(
        self,
        input_csv: Path | str,
        target_dir: Optional[Path | str] = None,
        randomization_params: Optional[RandomizationParams] = RandomizationParams(),
        tags_mapping_file: Optional[Path] = None,
        regex_paras_to_skip: Optional[list[str]] = REGEX_PARAS_TO_SKIP,
        **kwargs,
    ):
        self.input_csv = make_path_ok(input_csv)
        assert self.input_csv.exists()

        self.target_dir = get_dir_or_temp(target_dir)

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
                self.tags_mapping_file = get_file_or_temp(
                    self.target_dir, fn_if_needed=TAGS_MAPPING_FN
                )
            else:
                # we got one,  but it doesn't exist yet,
                self.tags_mapping_file = get_file_or_temp(
                    tags_mapping_file, fn_if_needed=TAGS_MAPPING_FN
                )

        # Will be created in run()
        self.tags = None

        self._get_randomization_params(randomization_params, **kwargs)

        self.regex_paras_to_skip = regex_paras_to_skip

    def _read_tm_from_file(self) -> None:
        """Try to read the tag mapping from file if provided.

        If successfull, will create self.tags, if not - won't.
        """
        if not self.tags_mapping_file:
            return
        if not self.tags_mapping_file.exists():
            return

        try:
            logger.info(
                f"Using existing  tags mapping file at {str(self.tags_mapping_file)}"
            )
            self.tags = TagsMapping.from_json_file(self.tags_mapping_file)
        except Exception as e:
            logger.warning(
                f"Had problems accessing tags mapping file {self.tags_mapping_file}, will create new one: {e}"
            )

    def create_or_read_tag_mapping(self):
        """Try reading tag mapping from file, use UP's website if that fails."""
        self._read_tm_from_file()

        # If the above didn't create a tag mapping fro whatever reason...
        if not self.tags:
            # Parse UP's tags pages to create a tags mapping w/o English tags
            self.tags = self.create_tag_mapping(
                randomization_params=self.randomization_params
            )
            self.save_tags_mapping()

    #  @staticmethod
    def parse_input(self, csv_path: Path):
        df = pd.read_csv(csv_path)

        full_articles = list()

        # Each group contains 1..3 translations of the same article
        grouped = df.groupby("id")

        num_articles_full = len(df)
        num_articles = len(grouped)

        logger.info(f"Reading {csv_path}")
        logger.info(
            f"Found {num_articles} articles ({num_articles_full} incl. translations) over {len(df.groupby('date'))} days"
        )
        num_existing_articles = len(
            [x for x in self.target_dir.iterdir() if x.is_dir() and x.name.isnumeric()]
        )
        if num_existing_articles:
            logger.info(
                f"The output directory may have {num_existing_articles} articles already downloaded"
            )

        with logging_redirect_tqdm():
            with tqdm(total=num_articles_full, desc="articles") as pbar:
                # For each group of translations
                #  for art_id, group in tqdm(grouped, leave=False, desc="articles"):
                for art_id, group in grouped:
                    logger.debug(
                        f"Processing article {art_id,','.join(list(group.lang))}"
                    )
                    self.process_group(
                        (art_id, group),
                        randomization_params=self.randomization_params,
                        target_dir=self.target_dir,
                        tags_mapping=self.tags,
                        pbar=pbar,
                        regex_paras_to_skip=self.regex_paras_to_skip,
                    )
                    #  full_articles.append(fa)
                    # Update tags mapping at the end of the group
                    self.save_tags_mapping(silent=True)
                logger.info(f"Successfully downloaded {len(full_articles)} articles")

    @staticmethod
    def process_group(
        artid_group: tuple,
        randomization_params: RandomizationParams,
        target_dir: Path,
        pbar,
        tags_mapping: Optional[TagsMapping] = None,
        use_downloaded_files_to_update_tags: bool = True,
        regex_paras_to_skip: Optional[list[str]] = None,
    ) -> None:
        artid, group = artid_group

        group_dir = target_dir / str(artid)
        mkdir(group_dir)

        logger.debug(f"Saving group {artid} to {group_dir}")

        fa_dict = dict()
        for i, art_row in group.iterrows():
            uri = art_row["uri"]
            lang = art_row["lang"]
            art_id = art_row["id"]
            date = art_row["date"]

            art_filename = (
                lang + "_" + base64.b64encode(uri.encode()).decode() + ".json"
            )
            art_path = group_dir / art_filename

            if art_path.exists():
                if use_downloaded_files_to_update_tags and tags_mapping:
                    # Update the tags mapping to use info from the downloaded article
                    art = Article.from_json_file(art_path)
                    UPCrawler.update_tags_mapping(
                        tags_mapping=tags_mapping,
                        tags=art.tags_full,
                        language=Language(lang),
                    )
                logger.debug(
                    f"Skipping {artid}/{lang} ({art_row['uri']}) as downloaded"
                )
                pbar.update()
                continue

            logger.debug(f"{i}/{len(group)}: {uri} ({lang})")
            art = UPCrawler.crawl_article_uri(
                uri=uri,
                regex_paras_to_skip=regex_paras_to_skip,
                randomization_params=randomization_params,
            )
            if not art:
                # if something went wrong
                continue
            art.lang = Language(lang)
            art.art_id = art_id
            art.date = date
            art.to_json_file(art_path, indent=4, ensure_ascii=False)

            # Update tags mapping - maybe we get a couple of English tags...
            if tags_mapping:
                UPCrawler.update_tags_mapping(
                    tags_mapping=tags_mapping,
                    tags=art.tags_full,
                    language=Language(lang),
                )

            fa_dict[Language(lang)] = art
            pbar.update()

        # TODO bad assumption that the date of all translations is the same, I should use UA only
        return

    @staticmethod
    def update_tags_mapping(tags_mapping: TagsMapping, tags, language: Language):
        """Updates the tags mapping with tags from an article.

        Goal: mostly get English tags that aren't easily parseable from UP's
        website.
        """
        for tag_short, tag_name, tag_link in tags:
            if tag_short not in tags_mapping.tags_mapping:
                logger.info(f"{tag_short} not in tags mapping, adding...")
                # can happen, e.g. [('kijiv', 'Киев', '/rus/tags/kijiv/')]
                tags_mapping.tags_mapping[tag_short] = dict()
            tags_mapping.tags_mapping[tag_short][language] = (tag_name, tag_link)

    def save_tags_mapping(self, silent: bool = False):
        """Saves tags mapping to json."""
        msg = f"Saving tags mapping to {self.tags_mapping_file}"
        if silent:
            logger.debug(msg)
        else:
            logger.info(msg)

        try:
            self.tags.to_json_file(self.tags_mapping_file, indent=4, ensure_ascii=False)
        except KeyboardInterrupt as e:
            # Still try to save the file?
            logger.error(
                f"Keyboardinterrupt during the saving of tags map, still saving..."
            )
            self.tags.to_json_file(self.tags_mapping_file, indent=4, ensure_ascii=False)
            raise e

    @staticmethod
    def save_group(group, target_dir: Path):
        artid, group = group

    def run(self):
        assert self.input_csv.exists()
        # Create a tag mapping
        self.create_or_read_tag_mapping()
        # Crawl the pages in the CSV
        r = self.parse_input(self.input_csv)

    ######
    # CRAWLING
    #####

    @staticmethod
    def parse_soup(
        soup, regex_paras_to_skip: Optional[list[str]] = None
    ) -> Optional[Article]:

        # If we got an error, pass return it up
        title = soup.find_all("h1")[0].text

        try:
            author_name = soup.find_all("span", attrs={"class": "post_author"})[
                0
            ].a.text
            # TODO - why the below version doesn't work?
            #  author_name = soup.find_all("span", class_="post_author")[0].a.text
        #  except IndexError:
        #  except IndexError, AttributeError:
        except Exception as e:
            # Older UP articles from the 00s have no authors!
            #  e.info("Failed to get author name of uri {uri}")
            #  raise e
            #  logger.debug(f"Failed getting author name of {uri}: {e}")
            logger.debug(f"Failed getting author name article {title}: {e}")
            author_name = None

        tags = list()
        #  tags_spans = soup.find_all("span", class_="post_tags_item")
        tags_spans = soup.find_all("span", attrs={"class": "post_tags_item"})

        # No tags in English version
        if tags_spans:
            for span in tags_spans:
                tag_name = span.text
                tag_link = span.a["href"]
                tag_short_name = UPCrawler._tag_name_from_link(tag_link)
                tags.append((tag_short_name, tag_name, tag_link))

        text_raw = soup.find_all("div", class_="post_text")[0]

        text_paras = text_raw.find_all(UPCrawler.PARAS_WITH_TEXT)

        text = list()

        # Add non-empty paragraphs as list of strings
        for para in text_paras:
            # if not-empty
            if para.text:
                # if not matching any of the bad regexes (if we set some)
                if regex_paras_to_skip:
                    flag = False
                    for r in regex_paras_to_skip:
                        if re.compile(r, re.IGNORECASE).match(para.text):
                            flag = True
                    if flag:
                        continue
                # Normalize to replace all nonbreakable space and friends
                #  see https://stackoverflow.com/questions/10993612/how-to-remove-xa0-from-string-in-python
                norm_text = normalize("NFKC", para.text).strip()
                if norm_text:
                    text.append(norm_text)

        article = Article(
            uri=None,  # WILL BE FILLED IN PARENT FUNCTION
            title=title,
            author_name=author_name,
            tags_full=tags,
            tags=[x[0] for x in tags],
            text=text,
            raw_html=text_raw,  # TODO isn't it better to save the ENTIRE page here?
        )
        return article

    @staticmethod
    def crawl_article_uri(
        uri: str,
        regex_paras_to_skip: Optional[list[str]] = None,
        #  tag_mapping: Optional[dict[str, dict[Language, tuple(str, str)]]],
        randomization_params: RandomizationParams = RandomizationParams(),
    ) -> Optional[Article]:
        """crawl_single_uri, with Article in one language

        Args:
            uri (str): uri
            regex_paras_to_skip: list of regexes, paragraphs matching any
                of them (case-insensitive) won't be added to article text

        Returns:
            Optional[Article]: None if there was a 404
        """
        soup = UPCrawler.do_basic_uri_ops_when_crawling(
            uri=uri, randomization_params=randomization_params
        )

        if not soup:
            return None

        article = UPCrawler.parse_soup(soup=soup, regex_paras_to_skip=regex_paras_to_skip)
        # ! TODO - ugly but better function separation that way. 
        #   https://chat.openai.com/share/cc613c43-193e-487c-a4c9-c42e180afb23
        article.uri = uri  

        return article

    @staticmethod
    def create_tag_mapping(
        randomization_params: RandomizationParams = RandomizationParams(),
    ) -> TagsMapping:
        """Parse UP's tag pages for UA and RU and create a dict with
        tags in both languages.
        """
        logger.info(f"Creating tag mapping from UP's website...")
        ua_tags = UPCrawler.crawl_tags_uri(
            uri=URI_TAGS_UA, randomization_params=randomization_params
        )
        ru_tags = UPCrawler.crawl_tags_uri(
            uri=URI_TAGS_RU, randomization_params=randomization_params
        )

        all_keys = set(ua_tags.keys()).union(ru_tags.keys())

        tags = dict()
        for k in all_keys:
            tag = dict()
            tag[Language.UA] = ua_tags.get(k, None)
            tag[Language.RU] = ru_tags.get(k, None)
            tags[k] = tag
        logger.info(f"Created tag mapping with {len(tags)} tags!")
        tm = TagsMapping(tags_mapping=tags)
        return tm

    @staticmethod
    def crawl_tags_uri(
        uri: str,
        randomization_params: RandomizationParams = RandomizationParams(),
    ) -> dict[str, tuple[str, str]]:
        """Crawls page with tags, e.g. https://www.pravda.com.ua/tags/,
        to get the tag names/uris and their names in the language
        of the crawled page.

        name = tserkva
        full_name = Церква
        uri = /eng/tags/tserkva/

        Args:
            uri (str): uri

        Returns:
            Dictionary name -> (full_name, uri)
        """
        soup = UPCrawler.do_basic_uri_ops_when_crawling(
            uri=uri, randomization_params=randomization_params
        )
        # If we got an error, pass return it up
        if not soup:
            return soup

        # short name -> plain language name, uri
        tags: dict[str, tuple[str, str]] = dict()
        div_tags_container = soup.find_all("div", class_="block_tags")[0]
        div_tags = div_tags_container.find_all("a")
        for t in div_tags:
            tag_name = t.text
            tag_link = t["href"]
            name_short = UPCrawler._tag_name_from_link(tag_link)
            tags[name_short] = (tag_name, tag_link)
        return tags

    @staticmethod
    def _tag_name_from_link(tag_link: str) -> str:
        """'/eng/tags/tserkva/' -> ''tserkva'"""
        return tag_link.split("/")[-2]

    ######
    # NETWORKING
    ######
    @retry(
        stop=stop_after_attempt(MAX_RETRIES_FOR_REQUEST),  # Maximum number of retries
        wait=wait_exponential(multiplier=1, min=1, max=60),  # Exponential backoff
        before_sleep=before_sleep_log(logger, logging.INFO),
        #  retry=retry_if_not_exception_type((ValueError))
        retry=retry_if_exception_type((ConnectionError, ReadTimeout)),
    )
    @staticmethod
    def do_basic_uri_ops_when_crawling(
        uri: str,
        randomization_params: Optional[RandomizationParams] = RandomizationParams(),
    ) -> Optional[BeautifulSoup]:
        """Gets the soup, or returns None if errors happened.

        Returns None if URI is 404 or got any HTTP code except 404
        Raise ValueError on 403

        Retry X times if networking issues happen.
        """

        logger.debug(f"Using randomization: {randomization_params}")

        # wait
        randomization_params.random_wait()

        # be polite
        useragent = randomization_params.get_useragent()
        headers = {"user-agent": useragent}
        #  logger.debug(f"Using headers: {headers}")

        website = requests.get(uri, headers=headers, timeout=(10, 10))

        if website.status_code != 200:
            if website.status_code != 404:
                logger.info(f"{uri} returned status code {website.status_code}")

            # Be a good scraper and fail loudly at the first sign of problems
            if website.status_code == 403:
                logger.error(f"403! {uri} returned status code {website.status_code}")
                raise ValueError("403")

            return None

        soup = BeautifulSoup(
            website.content, "html.parser", from_encoding=website.encoding
        )

        def is_404(website):
            if website.status_code == 404:
                return True

            title = soup.find_all("h1")[0].text
            # Inspired by https://www.pravda.com.ua/news/2022/03/15/7331466/
            errors = ["error 404", "ошибка 404", "помилка 404"]
            for e in errors:
                if e in title.lower():
                    return True
            return False

        if is_404(website):
            logger.debug(f"{uri} returned 404")
            return None
        logger.debug(f"Returning soup")
        return soup

    ######
    # RANDOM
    ######

    def _get_randomization_params(self, randomization_params, **kwargs) -> None:
        if randomization_params:
            self.randomization_params = randomization_params
        elif kwargs:
            self.randomization_params = RandomizationParams(**kwargs)
        else:
            self.randomization_params = RandomizationParams()
            logger.debug(f"Using RandomizationParams {self.randomization_params}")


def run_crawl(args):
    assert args.input, "Provide path to json with URIs to crawl"

    logger.info(f"Running with params {args}")
    logger.info(f"Crawling URIs from {args.input}")
    rw = _parse_timeout(args)
    #  res = crawl_all_uris(args.input, output_file=args.output, randomization_params=rw)
    cr = UPCrawler(
        input_csv=args.input,
        target_dir=args.output,
        randomization_params=rw,
        tags_mapping_file=args.tags_mapping_file,
    )
    cr.run()

    if args.pdb:
        breakpoint()


def parse_args_crawl() -> argparse.Namespace:
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
    parser.add_argument(
        "--tags_mapping_file",
        "-tm",
        help="Location of file with tags mapping, if present. (%(default)s)",
        type=Path,
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        default=5,
        help="""Max timeout when crawling articles, set to -1 to disable \
                all kinds of randomization. (%(default)s)""",
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
    args = parse_args_crawl()
    logger.setLevel(args.loglevel if args.loglevel else logging.INFO)

    logger.debug(args)

    try:
        run_crawl(args)
    except Exception as e:
        if args.pdb:
            extype, value, tb = sys.exc_info()
            traceback.print_exc()
            pdb.post_mortem(tb)
        else:
            logger.exception(e)


if __name__ == "__main__":
    main()
