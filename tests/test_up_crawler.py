import pytest
from datetime import datetime
from pathlib import Path

from up_crawler import __version__
from up_crawler.get_uris import UPSitemapCrawler
from up_crawler.bs_oop import UPCrawler
from up_crawler.path_ops import get_file_or_temp
from up_crawler.up_reader import UPReader, UPToCSVExporter
from up_crawler.consts import REGEX_PARAS_TO_SKIP

import logging

logging.basicConfig()
logger = logging.getLogger("test")

b = breakpoint

SMALL_CORPUS = Path(__file__).parent / "assets" / "2days_corpus"


def test_version():
    assert __version__ == "0.1.0"


def test_sitemap():
    day = datetime(year=2022, month=12, day=20)
    sitemap_uri = UPSitemapCrawler()._get_sitemap_uri_for_month(day)
    assert sitemap_uri == "https://www.pravda.com.ua/sitemap/sitemap-2022-12.xml.gz"


@pytest.mark.network
def test_uri_crawl_not_404():
    # Has 404 in the title but is not an error
    URI = "https://www.pravda.com.ua/news/2022/03/15/7331466/"
    res = UPCrawler.crawl_article_uri(uri=URI)
    assert res is not None


@pytest.mark.network
def test_uri_crawl_real_404():
    # Has 404 in the title but is not an error
    # real 404
    URI = "https://www.pravda.com.ua/news/2022/03/15/733146688/"
    res = UPCrawler.crawl_article_uri(uri=URI)
    assert res is None


@pytest.mark.network
@pytest.mark.now
def test_ski_paras():
    """Tests whether skipping paragraphs by regex works"""
    URI = "https://www.pravda.com.ua/eng/news/2023/12/10/7432464/"
    res = UPCrawler.crawl_article_uri(uri=URI)
    # THEY LITERALLY EDITED THAT LINE OUT YESTERDAY
    #  snippet = "completion of the rescue operation"
    snippet = "evacuation mission"
    res_skip = UPCrawler.crawl_article_uri(
        uri=URI, regex_paras_to_skip=[f".*{snippet}.*"]
    )

    assert snippet in res.get_text()
    assert snippet not in res_skip.get_text()


@pytest.mark.network
def test_skip_specific_paras():
    PARAS = [
        "Читайте также: Внимание, мины! Как война превратила Украину в большое минное поле, и что с этим делать",  # https://www.pravda.com.ua/rus/news/2023/11/14/7428733/
        "Читайте також: Увага, міни! Як війна перетворила Україну на велике мінне поле, і що з цим робити",
        "Support UP or become our patron!",  # https://www.pravda.com.ua/eng/news/2023/11/15/7428868/
        "Ukrainska Pravda is the place where you will find the most up-to-date information about everything related to the war in Ukraine. Follow us on Twitter, support us, or become our patron!",  # https://www.pravda.com.ua/eng/news/2023/09/27/7421639/
    ]

    URIS = [
        "https://www.pravda.com.ua/eng/news/2023/09/27/7421639/",
        "https://www.pravda.com.ua/eng/news/2023/11/15/7428868/",
        "https://www.pravda.com.ua/rus/news/2023/11/14/7428733/"
    ]

    arts = list()
    arts_skip = list()

    for u in URIS:
        art = UPCrawler.crawl_article_uri(uri=u, regex_paras_to_skip=REGEX_PARAS_TO_SKIP)
        art_skip = UPCrawler.crawl_article_uri(uri=u)
        arts.append(art)
        arts_skip.append(art_skip)

    for i in range(len(arts)):
        res_skip = list()
        for p in PARAS:
            # no paragraphs should be in the clean version
            assert p not in arts[i].text[-1]
            res_skip.append([p in arts_skip[i].text[-1]])
        # at least one paragraph should be in the dirty version
        assert any(res_skip)


"""
TODO:
- 7430996 has diff tags in diff l anguages
DONE
- https://www.pravda.com.ua/rus/news/2023/11/12/7428388/ has literally one picture and no content
- https://www.pravda.com.ua/news/2022/03/15/7331466/ has 404 in the title
"""

@pytest.mark.network
def test_empty_art():
    URI = "https://www.pravda.com.ua/rus/news/2023/11/12/7428388/"
    res = UPCrawler.crawl_article_uri(uri=URI)
    assert res.text == list()
    assert res.get_text()==""


@pytest.mark.skip
def test_csv():
    # TODO
    fas = [x for x in UPReader.read_dir_chunked(path=SMALL_CORPUS)]
    #  target_file = get_file_or_temp(fn_if_needed="testtest.csv")
    target_file = get_file_or_temp(Path("/tmp/myout.csv"))
    r = UPToCSVExporter.fas_to_csv(fas=fas, target_csv=target_file)

@pytest.mark.network
@pytest.mark.skip
def test_correct_tags():
    URI = "https://www.pravda.com.ua/news/2023/08/10/7414962/"
    res = UPCrawler.crawl_article_uri(uri=URI)
    res_skip = UPCrawler.crawl_article_uri(uri=URI)
    # TODO

