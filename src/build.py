import asyncio
import gzip
import json
from dataclasses import dataclass, field
from io import BytesIO
from aiohttp import ClientSession
from multiprocessing import Pool
from bs4 import BeautifulSoup
from lxml import etree
from redis_stuff import DOCS_DATABASE, KEYWORDS_INDEX, ABSTRACTS_INDEX, TITLES_INDEX, \
    CHEMICALS_INDEX, AUTHOR_INDEX, DATABASES_TO_PROCESS, SESSIONS
from vector import preprocess_text, make_words_vector

PUBMED_MAIN_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"

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


def docs_database_builder(article_data: ArticleData):
    session = SESSIONS[DOCS_DATABASE]
    article_id = [article_id for article_id in article_data.article_ids
                  if article_id.attrs.get("IdType") == "pubmed"][0].text
    session.set(article_id, json.dumps(
        {
            "title": article_data.titles[0].text if article_data.titles else None,
            "language": article_data.language[0].text if article_data.language else None,
            "article_url": f"https://pubmed.ncbi.nlm.nih.gov/{article_id}/",
            "article_ids": {a_id.attrs.get("IdType"): a_id.text for a_id in article_data.article_ids}
        }
    ))


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
        return [ArticleDataItem(text=root.text, attrs=dict(root.attrib))]


async def data_reader(max_files=0, files_offset=0):
    articles_counter = 0
    session = ClientSession()
    urls = await get_files_url(session)
    urls = urls[files_offset:] if not max_files else urls[files_offset: files_offset + max_files]

    for url in urls:
        print(f"getting file {url}")
        resp = await session.get(url)
        compressed_file = BytesIO(await resp.content.read())
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
            if articles_counter % 10000 == 0:
                print(f"Processed {articles_counter} articles")
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
        if articles_counter % 100 == 0:
            print(f"Processed {articles_counter} articles")
    print("Building finished!")


def worker():
    asyncio.get_event_loop().run_until_complete(build_indices(max_files=0, files_offset=103))


if __name__ == "__main__":
    worker()