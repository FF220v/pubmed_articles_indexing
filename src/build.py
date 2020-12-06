import asyncio
import gzip
import json
from dataclasses import dataclass, field
from io import BytesIO, StringIO
from multiprocessing import Pool

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from lxml import etree

from http_session import get_client_session
from redis_stuff import DOCS_DATABASE, KEYWORDS_INDEX, ABSTRACTS_INDEX, TITLES_INDEX, \
    CHEMICALS_INDEX, AUTHOR_INDEX, DATABASES_TO_PROCESS, SESSIONS
from vector import preprocess_text, make_words_vector

PUBMED_MAIN_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"

MAX_LINKS = 10

REFERENCES_LINK_TYPE = "pubmed_pubmed_refs"
SIMILAR_LINK_TYPE = "pubmed_pubmed"
CITEDIN_LINK_TYPE = "pubmed_pubmed_citedin"


MAJOR_TOPIC_MULTIPLIER = 2


def preprocess_article_data_items(items):
    words = []
    for item in items:
        processed_words = preprocess_text(item.text)
        multiplier = MAJOR_TOPIC_MULTIPLIER if item.attrs.get("MajorTopicYN") == "Y" else 1
        words.extend(processed_words * multiplier)
    return words


def write_doc_to_index(index, doc_id, words_vector):
    session = SESSIONS[index]
    words = list(words_vector.keys())
    response = session.mget(words_vector.keys())
    update_dict = {}
    for word, word_dict in zip(words, response):
        if not word_dict:
            update_dict[word] = {}
        else:
            update_dict[word] = json.loads(word_dict)
        update_dict[word][doc_id] = words_vector[word]
    if update_dict:
        session.mset({k: json.dumps(v) for k, v in update_dict.items()})
    return True


@dataclass
class ArticleDataItem:
    text: str = field(default_factory=str)
    attrs: dict = field(default_factory=dict)


@dataclass
class ArticleData:
    article_ids: list = field(default_factory=list)
    keywords: list = field(default_factory=list)
    abstracts: list = field(default_factory=list)
    titles: list = field(default_factory=list)
    chemicals: list = field(default_factory=list)
    language: list = field(default_factory=list)
    authors: list = field(default_factory=list)


def get_index_builder(index: int, field: str):
    def builder_base(article_data: ArticleData):
        article_id = [article_id for article_id in article_data.article_ids
                      if article_id.attrs.get("IdType") == "pubmed"][0].text
        items = article_data.__getattribute__(field)
        words_to_index = preprocess_article_data_items(items)
        words_vector = make_words_vector(words_to_index)
        write_doc_to_index(index, article_id, words_vector)
        return True

    return builder_base


def async_wait_and_retry(coro):
    async def wrapper(*args, **kwargs):
        trial = 1
        while True:
            try:
                return await coro(*args, **kwargs)
            except Exception as e:
                if trial < 10:
                    if isinstance(e, Non200Exception):
                        wait_seconds = trial * 0.25
                    else:
                        wait_seconds = 0
                    print(f"Exception: {e}, Trial {trial}. Waiting {wait_seconds} s.")
                    await asyncio.sleep(wait_seconds)
                    trial += 1
                else:
                    raise e
    return wrapper


class Non200Exception(Exception):
    pass


@async_wait_and_retry
async def get_links(url):
    session = get_client_session()
    resp = await session.get(url)
    if resp.status != 200:
        raise Non200Exception(f"Got status {resp.status}")
    return resp


async def get_links_by_article(article_id, link_type, return_type_name):
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&linkname={link_type}&id={article_id}"
    resp = await get_links(url)
    resp_text = await resp.text()
    parser = etree.XMLParser(ns_clean=True)
    tree = etree.parse(BytesIO(bytes(resp_text, 'utf-8')), parser).getroot()
    links = [make_pubmed_link(l.text) for l in find_data_recursive(tree, ["LinkSet", "LinkSetDb", "Link", "Id"])]
    return return_type_name, links[:MAX_LINKS]


def make_pubmed_link(article_id):
    return f"https://pubmed.ncbi.nlm.nih.gov/{article_id}/"


def docs_database_builder(article_data: ArticleData):
    session = SESSIONS[DOCS_DATABASE]
    article_id = [article_id for article_id in article_data.article_ids
                  if article_id.attrs.get("IdType") == "pubmed"][0].text

    results = asyncio.get_event_loop().run_until_complete(
        asyncio.gather(get_links_by_article(article_id, CITEDIN_LINK_TYPE, "cited_in"),
                       get_links_by_article(article_id, REFERENCES_LINK_TYPE, "references"),
                       get_links_by_article(article_id, SIMILAR_LINK_TYPE, "similar_articles")))
    results = dict(results)

    res_dict = {
        "title": article_data.titles[0].text if article_data.titles else None,
        "language": article_data.language[0].text if article_data.language else None,
        "article_url": make_pubmed_link(article_id),
        "article_ids": {a_id.attrs.get("IdType"): a_id.text for a_id in article_data.article_ids},
    }
    res_dict.update(results)

    session.set(article_id, json.dumps(res_dict))


BUILDERS = {
    KEYWORDS_INDEX: get_index_builder(KEYWORDS_INDEX, "keywords"),
    ABSTRACTS_INDEX: get_index_builder(ABSTRACTS_INDEX, "abstracts"),
    TITLES_INDEX: get_index_builder(TITLES_INDEX, "titles"),
    CHEMICALS_INDEX: get_index_builder(CHEMICALS_INDEX, "chemicals"),
    AUTHOR_INDEX: get_index_builder(AUTHOR_INDEX, "authors"),
    DOCS_DATABASE: docs_database_builder
}


async def get_files_url(session):
    resp = await session.get(PUBMED_MAIN_URL)
    print(f"getting url list from {PUBMED_MAIN_URL}")
    html_text = await resp.text()
    bs = BeautifulSoup(html_text, 'html.parser')
    a_tags = bs.find_all("a")
    results = []
    for a_tag in a_tags:
        ref = a_tag.attrs.get("href", "")
        if ref.startswith("pubmed") and ref.endswith(".xml.gz"):
            results.append(f"{PUBMED_MAIN_URL}{ref}")
    return results


def find_data_recursive(root, path: list):
    if path:
        results = []
        for root in root.findall(path[0]):
            results.extend(find_data_recursive(root, path[1:]))
        return results
    else:
        return [ArticleDataItem(text=root.text, attrs=dict(root.attrib))] if root.text is not None else []


async def data_reader(max_files=0, files_offset=0):
    articles_counter = 0
    session = get_client_session()
    urls = await get_files_url(session)
    urls = urls[files_offset:] if not max_files else urls[files_offset: files_offset + max_files]

    for url in urls:
        print(f"getting file {url}")
        resp = await session.get(url)
        compressed_file = BytesIO(await resp.content.read())
        print(f"got file {url}")
        decompressed_file = gzip.GzipFile(fileobj=compressed_file)
        parser = etree.XMLParser(ns_clean=True)
        tree = etree.parse(decompressed_file, parser).getroot()
        for article in tree.iterfind("PubmedArticle"):
            article_data = ArticleData(
                article_ids=find_data_recursive(article, ["PubmedData", "ArticleIdList", "ArticleId"]),
                titles=find_data_recursive(article, ["MedlineCitation", "Article", "ArticleTitle"]),
                language=find_data_recursive(article, ["MedlineCitation", "Article", "Language"]),
                abstracts=find_data_recursive(article, ["MedlineCitation", "Article", "Abstract", "AbstractText"]),
                chemicals=find_data_recursive(article, ["MedlineCitation", "ChemicalList", "Chemical", "NameOfSubstance"]),
                keywords=find_data_recursive(article, ["MedlineCitation", "Article", "KeywordList", "Keyword"]) +
                         find_data_recursive(article, ["MedlineCitation", "MeshHeadingList", "MeshHeading", "DescriptorName"]) +
                         find_data_recursive(article, ["MedlineCitation", "MeshHeadingList", "MeshHeading", "QualifierName"]),
                authors=find_data_recursive(article, ["MedlineCitation", "Article", "AuthorList", "Author", "LastName"]) +
                        find_data_recursive(article, ["MedlineCitation", "Article", "AuthorList", "Author", "ForeName"])
            )
            articles_counter += 1
            if articles_counter % 10 == 0:
                print(f"Yielded {articles_counter} articles")
            yield article_data


def index_processor(args):
    try:
        index, item = args
        BUILDERS[index](item)
    except Exception as e:
        print("Exception occured!")
        print(e)


async def build_indices(max_files=0, files_offset=0):
    index_builders_pool = Pool(processes=len(DATABASES_TO_PROCESS))
    articles_counter = 0
    async for item in data_reader(max_files=max_files, files_offset=files_offset):
        args = []
        for index in DATABASES_TO_PROCESS:
            args.append((index, item))
        index_builders_pool.map(index_processor, args)
        articles_counter += 1
        if articles_counter % 10 == 0:
            print(f"Processed {articles_counter} articles")
    print("Building finished!")
    index_builders_pool.close()


def worker():
    asyncio.get_event_loop().run_until_complete(build_indices(max_files=0, files_offset=150))


if __name__ == "__main__":
    worker()