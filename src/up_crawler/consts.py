import re

# updated, extended: https://regex101.com/r/dYlIiF/4
URI_REGEX_STR_EXT = r"(?P<uri>(?P<domain>.*\.com\.ua\/)(?P<lang>(eng)|(rus))?\/?(?P<kind>.*?)\/(?P<art_id>.*(?P<date_part>....\/..\/..?)\/(?P<id>.*)\/))"
URI_REGEX_EXT = re.compile(URI_REGEX_STR_EXT)

# Domains links from which will be crawled when found. Skip e.g. epravda etc.
SUPPORTED_DOMAINS = ["www.pravda.com.ua"]

# When requests fail more than this, raise exception and crash loudly
MAX_RETRIES_FOR_REQUEST = 10

# ENG tags exist but no page for them: https://www.pravda.com.ua/eng/tags/zelensky/ = https://www.pravda.com.ua/tags/zelensky/
# the 'all topics' link is broken too, haha: https://www.pravda.com.ua/tags/
URI_TAGS_UA = "https://www.pravda.com.ua/tags/"
URI_TAGS_RU = "https://www.pravda.com.ua/rus/tags/"

TAGS_MAPPING_FN = "tags_mapping.json"
URIS_TOCRAWL_FN = "uris.csv"


# Paragraphs containing this text won't be added to article text, case insensitive
PARAS_TO_SKIP = [
    "Follow (us|Ukrainska Pravda) on Twitter",
    "Support UP",
    "become our patron",
    "(читайте|слухайте|слушайте) (також|также)", 
]

# TODO test this
REGEX_PARAS_TO_SKIP = [f".*{x}.*" for x in PARAS_TO_SKIP]
