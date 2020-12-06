import json
from collections import defaultdict
from multiprocessing import Pool
from redis_stuff import INDICES_TO_PROCESS, SESSIONS, AUTHOR_INDEX, ABSTRACTS_INDEX,\
    KEYWORDS_INDEX, CHEMICALS_INDEX, TITLES_INDEX, DOCS_DATABASE
from vector import preprocess_text, make_words_vector

INDICES_WEIGHTS = {
    AUTHOR_INDEX: 4,
    ABSTRACTS_INDEX: 2,
    KEYWORDS_INDEX: 2,
    CHEMICALS_INDEX: 2,
    TITLES_INDEX: 3
}


def find_vectors_similarity(vector_a, vector_b):
    words_to_calc = set(vector_a).intersection(set(vector_b))
    return sum(vector_a[w] * vector_b[w] for w in words_to_calc)


def find_doc_vectors_in_index(index: int, words_to_get: list):
    session = SESSIONS[index]
    doc_vectors = defaultdict(dict)
    index_data = session.mget(words_to_get)
    for word, data in zip(words_to_get, index_data):
        if data:
            data = json.loads(data)
            for k, v in data.items():
                doc_vectors[k][word] = v
    return doc_vectors


def find_similarity_worker(args):
    index, word_vector = args
    doc_vectors = find_doc_vectors_in_index(index, list(word_vector.keys()))
    docs_similarity_dict = dict()
    for doc, vector in doc_vectors.items():
        docs_similarity_dict[doc] = find_vectors_similarity(vector, word_vector)
    return index, docs_similarity_dict


def analyse_keywords(keywords):
    word_vector = make_words_vector(preprocess_text(keywords))
    similarity_search_process_pool = Pool(len(INDICES_TO_PROCESS))
    args = []
    for index in INDICES_TO_PROCESS:
        args.append((index, word_vector))
    results = similarity_search_process_pool.map(find_similarity_worker, args)
    similarity_search_process_pool.close()
    docs_scores = defaultdict(float)
    weights_sum = sum(INDICES_WEIGHTS.values())
    for index, docs_similarity_dict in results:
        for doc, score in docs_similarity_dict.items():
            docs_scores[doc] += score * INDICES_WEIGHTS[index] / weights_sum

    docs_most_relevant = sorted(list((k, v) for k, v in docs_scores.items()), key=lambda x: x[1], reverse=True)[:30]
    keys_to_query = [k[0] for k in docs_most_relevant]
    results = [json.loads(res) if res is not None else {} for res in SESSIONS[DOCS_DATABASE].mget(keys_to_query)]
    for result, doc in zip(results, docs_most_relevant):
        result['score'] = round(doc[1], 5)
    file_data = json.dumps(results, indent=2)

    with open("report.json", mode="w+") as f:
        f.write(file_data)
    print(file_data)
    print("Saved report to file!")


def worker(keywords):
    analyse_keywords(keywords)


if __name__ == "__main__":
    worker("lactose intolerance")
