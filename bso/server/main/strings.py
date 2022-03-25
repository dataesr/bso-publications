"""String utils."""
import re
import string
import unicodedata

from tokenizers import normalizers
from tokenizers.normalizers import BertNormalizer, Sequence, Strip
from tokenizers import pre_tokenizers
from tokenizers.pre_tokenizers import Whitespace
from bso.server.main.logger import get_logger

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


normalizer = Sequence([BertNormalizer(clean_text=True,
        handle_chinese_chars=True,
        strip_accents=True,
        lowercase=True), Strip()])
pre_tokenizer = pre_tokenizers.Sequence([Whitespace()])

def normalize(x, min_length = 0):
    normalized = normalizer.normalize_str(x)
    normalized = normalized.replace('\n', ' ')
    normalized = re.sub(' +', ' ', normalized)
    return " ".join([e[0] for e in pre_tokenizer.pre_tokenize_str(normalized) if len(e[0]) > min_length])

def normalize2(x, remove_space = True, min_length = 0):
    if not isinstance(x, str):
        return ''
    normalized = normalizer.normalize_str(x)
    normalized = normalized.replace('\n', ' ')
    normalized = re.sub(' +', ' ', normalized)
    normalized = remove_punction(normalized)
    normalized = " ".join([e[0] for e in pre_tokenizer.pre_tokenize_str(normalized) if len(e[0]) > min_length])
    if remove_space:
        normalized = normalized.strip().replace(' ', '')
    return normalized

#def normalize(x: str) -> str:
#    y = normalizer.normalize_str(delete_punct(x))
#    y = y.replace('\n', ' ')
#    return re.sub(' +', ' ', y).strip()


def get_words(x):
    if isinstance(x, str):
        return x
    elif isinstance(x, dict):
        return get_words([get_words(w) for w in list(x.values())])
    elif isinstance(x, list):
        return ' '.join([get_words(w) for w in x])
    else:
        #logger.debug(f'Get_words is called on {type(x)} object when it should be a str, list or dict !')
        return ''
