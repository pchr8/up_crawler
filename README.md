## NOTE
The script is done in the context of my Master's Thesis.

The script works but is still a work in progress, I'll add better documentation and tests
later on as I'll be nearing to the end of my thesis, if time allows.

## The dataset
The last 2 years of articles in CSV format are uploaded to the HF Hub: [shamotskyi/ukr_pravda_2y · Datasets at Hugging Face](https://huggingface.co/datasets/shamotskyi/ukr_pravda_2y)

The script generates .json files that contain additional info, like the raw HTML
of the articles. They were omitted from the CSV version above, contact me if interested.

## Ukrainska Pravda Crawler
Download [Українська правда](https://www.pravda.com.ua/) articles from a range of dates, 
including all translations available.

Create unified list of tags, with all translations for each of them as well.

## Howto
### Install
- **TODO** Install the package ... 
- `python3 -m up_crawler -h`

### Use 
```python
> python3 -m up_crawler -ds 'four weeks ago' -de 'three weeks ago' -o /tmp/your/output/folder
[15:48:12] INFO     Running with params Namespace(date_start='four      __main__.py:85
                    weeks ago', date_end='three weeks ago',
                    output=PosixPath('/tmp/your/output/folder'),
                    timeout=5, pdb=False, loglevel=None)
           INFO     Getting URLs of articles published between         get_uris.py:174
                    2023-11-12 ('four weeks ago') and 2023-11-19
                    ('three weeks ago')
[15:48:13] INFO     Getting                                            sitemaps.py:536
                    https://www.pravda.com.ua/sitemap/sitemap-2023-11.
                    xml.gz
           INFO     Got 1305 article URLs!                             get_uris.py:205
           INFO     Saved df to /tmp/your/output/folder/uris.csv       get_uris.py:147
           INFO     Creating tag mapping from UP's website...            bs_oop.py:360
[15:48:16] INFO     Created tag mapping with 1388 tags!                  bs_oop.py:376
           INFO     Saving tags mapping to                               bs_oop.py:252
                    /tmp/your/output/folder/tags_mapping.json
           INFO     Reading /tmp/your/output/folder/uris.csv             bs_oop.py:147
           INFO     Found 542 articles (1305 incl. translations) over 7  bs_oop.py:148
                    days
articles:   1%|▏                                   | 7/1305 [00:22<1:16:00,  3.51s/it]
```

### Output format
In the output directory, a directory is created for each article. 

This can be converted to a CSV representation by running `up_convert`.

#### Articles - main storage format
Each article has 
one to three files named like `eng_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9lbmcvbmV3cy8yMDIzLzExLzEzLzc0Mjg0NjQv.json` - 
`eng` is the language, the rest is a base64 representation of the URI of the page.

```
> tree /tmp/your/output/folder
/tmp/your/output/folder
├── 7428464
│   ├── eng_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9lbmcvbmV3cy8yMDIzLzExLzEzLzc0Mjg0NjQv.json
│   ├── rus_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9ydXMvbmV3cy8yMDIzLzExLzEzLzc0Mjg0NjQv.json
│   └── uk_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9uZXdzLzIwMjMvMTEvMTMvNzQyODQ2NC8=.json
├── 7428472
│   ├── eng_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9lbmcvbmV3cy8yMDIzLzExLzEzLzc0Mjg0NzIv.json
│   ├── rus_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9ydXMvbmV3cy8yMDIzLzExLzEzLzc0Mjg0NzIv.json
│   └── uk_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9uZXdzLzIwMjMvMTEvMTMvNzQyODQ3Mi8=.json
├── 7428483
│   ├── eng_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9lbmcvbmV3cy8yMDIzLzExLzEzLzc0Mjg0ODMv.json
│   ├── rus_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9ydXMvbmV3cy8yMDIzLzExLzEzLzc0Mjg0ODMv.json
│   └── uk_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9uZXdzLzIwMjMvMTEvMTMvNzQyODQ4My8=.json
├── 7428484
│   ├── rus_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9ydXMvbmV3cy8yMDIzLzExLzEzLzc0Mjg0ODQv.json
│   └── uk_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9uZXdzLzIwMjMvMTEvMTMvNzQyODQ4NC8=.json
├── 7428485
│   ├── eng_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9lbmcvbmV3cy8yMDIzLzExLzEzLzc0Mjg0ODUv.json
│   ├── rus_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9ydXMvbmV3cy8yMDIzLzExLzEzLzc0Mjg0ODUv.json
│   └── uk_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9uZXdzLzIwMjMvMTEvMTMvNzQyODQ4NS8=.json
├── 7428486
│   └── eng_aHR0cHM6Ly93d3cucHJhdmRhLmNvbS51YS9lbmcvbmV3cy8yMDIzLzExLzEzLzc0Mjg0ODYv.json
├── tags_mapping.json
└── uris.csv
```

#### Other files
- `tags_mapping.json` contains all tags used in all translations available.
- `uris.csv` has a list of all articles+translations published in the range of dates given, the ones that are to be downloaded


## Limitations
Downloads only articles older than about 15 days, since newer articles aren't  available through UP's archive sitemaps. 
