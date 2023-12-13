from pathlib import Path

from dataclass_wizard import JSONSerializable, JSONWizard, JSONFileWizard

import dateparser
from datetime import datetime

import logging

from dataclasses import dataclass
from collections import namedtuple
from enum import Enum

import rich.repr


from typing import List, Tuple, Optional, Dict, Union

logging.basicConfig()
logger = logging.getLogger(__package__)


class Language(str, Enum):
    # ISO 639-3 codes; rus/eng as used in UP's URI,  ukr to match the pattern
    # Same codes as used in UPSitemapCrawler's dataframe output
    UA = "ukr"
    RU = "rus"
    EN = "eng"


@dataclass
class Article(JSONSerializable, JSONFileWizard):
    """Single article in ONE language, corresponding to a single URI"""

    uri: str
    title: str
    author_name: str

    # List of paragraphs, each a string. See self.get_text() for single string
    # May be an empty list! https://www.pravda.com.ua/rus/news/2023/11/12/7428388/
    text: List[str]

    # Raw article content
    raw_html: Optional[str] = None

    # Language this specific article is in
    lang: Optional[Language] = None

    # The part after the /eng/ bit, without the leading slash but with trailing slash
    # Matches across translations of the same article
    # System is the same across UP(econ, euroint. etc.) versions
    art_id: Optional[str] = None

    # YYYY-MM-DD
    date: Optional[str] = None

    # Not present in UP-ENG, otherwise tag_link matches across UA/RU UPs
    tags_full: Optional[
        List[Tuple[str, str, str]]
    ] = None  # tag_short_name, tag_name, tag_link
    tags: Optional[List[str]] = None

    def get_text(self):
        """Get the article text as single string."""
        return " ".join(self.text)

    def __rich_repr__(self) -> rich.repr.Result:
        yield "ID", self.art_id,
        yield "lang", self.lang.name
        yield "URI", self.uri
        yield "Title", self.title
        yield "Author", self.author_name
        yield self.get_text()
        yield "Tags", self.tags_full

    __rich_repr__.angular = True


@dataclass
class TagsMapping(JSONSerializable, JSONFileWizard):
    """All known tags with all info about them, in all langs."""

    """
    o['pozhezha']= 
    {'uk': ['пожежа', '/tags/pozhezha/'], 'rus': ['пожар', '/rus/tags/pozhezha/'], 'eng': ['fire', '/eng/tags/pozhezha/']}
    """
    #  tags_mapping: dict = None
    #  tags_mapping: dict[str, dict[str, tuple[str, str]]]= None
    #  tags_mapping: dict[str, dict[Language, tuple[str, str]]] = None
    tags_mapping: Optional[dict[str, dict[Language, Optional[tuple[str, str]]]]] = None

    def save(self, path: Path):
        """Save preserving cyrillic."""
        path.write_text(
            self.to_json(indent=4, ensure_ascii=False),
            encoding="utf8",
        )


@rich.repr.auto(angular=True)
@dataclass
class FullArticle(JSONSerializable, JSONFileWizard):
    """Article containing all translations and unified tags"""

    art_id: int
    # Translations may be published on diff days, no guarantees this is right for all
    date_published: str
    # short tag names for all tags used in the article
    tags: list[str]
    articles: dict[Language, Article]

    def get_name(self, language: str | Language) -> str:
        lang = Language(language) if isinstance(language, str) else language
        return self.articles[lang]

    """
    def __rich_repr__(self) -> rich.repr.Result:
        yield self.art_id
        yield "Date", self.date_published
        yield "tags", self.tags 
        # TODO bad assumption that UA article will always be present
        #  yield "title", self.articles[Language.UA.value].title
        yield "title", list(self.articles.values())[0].title
        yield self.articles
    """
