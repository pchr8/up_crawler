import pdb
import sys
import traceback
import argparse

import logging
from rich.logging import RichHandler

logging.basicConfig(
    level="NOTSET",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[
        RichHandler(
            #  show_path=False
            #  rich_tracebacks=True,
        )
    ],
)
logger = logging.getLogger(__package__)

from pathlib import Path


from datetime import datetime

import rich
from rich import inspect, print

from typing import List, Tuple, Optional, Dict, Union

from up_crawler.data_structures import (
    Language,
    Article,
    TagsMapping,
)

from up_crawler.randomization import RandomizationParams, _parse_timeout

from up_crawler.get_uris import UPSitemapCrawler
from up_crawler.path_ops import get_file_or_temp, get_dir_or_temp
from up_crawler.bs_oop import UPCrawler
from up_crawler.consts import URIS_TOCRAWL_FN



class FullUPCrawler:
    """Get a range of dates from which to download articles.

    Get the URIs of articles published on those days from UP sitemap,
    then crawl those articles.

    Serialize all downloaded articles as json files in the target directory,
    one dir for article containing 1..3 jsons for each traslation.

    Save the map of tags (with all available translations) in the same target
    directory.
    """

    def parse_and_download_everything(
        self,
        d1: Union[datetime, str],
        d2: Optional[Union[datetime, str]] = "yesterday",
        target_dir: Optional[Path | str] = None,
        randomization_params: Optional[RandomizationParams] = RandomizationParams(),
    ):
        # Sitemap magic
        us = UPSitemapCrawler()
        target_path = get_dir_or_temp(target_dir)
        csv_path =get_file_or_temp(path = target_dir, fn_if_needed=URIS_TOCRAWL_FN)
        # TODO hypothetically reuse the DF in target_dir if present, but not worth it
        df_path = us.get_and_save_article_uris(d1=d1, d2=d2, save_path=csv_path)

        uc = UPCrawler(
            input_csv=df_path,
            target_dir=target_dir,
            randomization_params=randomization_params,
        )
        uc.run()
        logger.info(f"Successfully downloaded all articles!")




def run(args):
    logger.info(f"Running with params {args}")

    date_1 = args.date_start  # if d1 else "3 days ago"
    date_2 = args.date_end  # if d2 else 'yesterday'
    output_path = args.output

    rw = _parse_timeout(args)
    fup = FullUPCrawler()
    fup.parse_and_download_everything(
        d1=date_1,
        d2=date_2,
        target_dir=args.output,
        randomization_params=rw,
    )


def parse_args() -> argparse.Namespace:
    DEFAULT_START_DATE = "three days ago"
    DEFAULT_END_DATE = "yesterday"

    parser = argparse.ArgumentParser()
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
    parser.add_argument(
        "--output",
        "-o",
        help="Output for the dataset (%(default)s)",
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
#   - "I'm running, and I haven't crawled stuff for 5 days, I'll go through the 5 days" or something
