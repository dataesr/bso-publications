"""String utils."""
import re
import string
import unicodedata

from bso.server.main.logger import get_logger
from tokenizers.normalizers import BertNormalizer, Sequence, Strip

logger = get_logger(__name__)

def dedup_sort(x: list) -> list:
    y = list(set([e for e in x if e]))
    y.sort()
    return y


def remove_punction(s: str) -> str:
    for p in string.punctuation:
        s = s.replace(p, ' ').replace('  ', ' ')
    return s.strip()


def strip_accents(w: str) -> str:
    """Normalize accents and stuff in string."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', w)
        if unicodedata.category(c) != 'Mn')


def delete_punct(w: str) -> str:
    """Delete all punctuation in a string."""
    return w.lower().translate(
        str.maketrans(string.punctuation, len(string.punctuation) * ' '))


normalizer = Sequence([BertNormalizer(), Strip()])


def normalize(x: str) -> str:
    y = normalizer.normalize_str(delete_punct(x))
    y = y.replace('\n', ' ')
    return re.sub(' +', ' ', y).strip()


def get_words(x):
    if isinstance(x, str):
        return x
    elif x is None:
        return ''
    elif isinstance(x, dict):
        return get_words([get_words(w) for w in list(x.values())])
    elif isinstance(x, list):
        return " ".join([get_words(w) for w in x])
    else:
        logger.debug(f"get_words is called on {type(x)} object when it should be a str, list or dict !")
        return ""
