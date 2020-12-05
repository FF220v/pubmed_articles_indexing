from math import sqrt
from string import punctuation
from typing import List
import nltk
from nltk import word_tokenize
from nltk.corpus import stopwords

nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

excluded_tokens = set(w.lower() for w in set(stopwords.words('english')).union(set(punctuation)))


def preprocess_text(text: str) -> List[str]:
    return [w.lower() for w in word_tokenize(text) if w not in excluded_tokens]


def make_words_vector(words: List[str]) -> dict:
    vector = {w: words.count(w) for w in words}
    module = sqrt(sum(value**2 for value in vector.values()))
    return {k: round(v/module, 5) for k, v in vector.items()}  # Normalized vector
