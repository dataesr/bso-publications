"""String utils."""
import string
import unicodedata
import collections
from typing import Dict
from tokenizers import normalizers
from tokenizers.normalizers import Lowercase, NFD, StripAccents, Strip, BertNormalizer
import re

def dedup_sort(x):
    y = list(set([e for e in x if e]))
    y.sort()
    return y

def remove_punction(s):
    for p in string.punctuation:
        s=s.replace(p,' ').replace('  ',' ')
    return s.strip()

def strip_accents(w: str) -> str:
    """Normalize accents and stuff in string."""
    return "".join(
        c for c in unicodedata.normalize("NFD", w)
        if unicodedata.category(c) != "Mn")


def delete_punct(w: str) -> str:
    """Delete all puctuation in a string."""
    return w.lower().translate(
        str.maketrans(string.punctuation, len(string.punctuation)*" "))

normalizer = normalizers.Sequence([BertNormalizer(), Strip()])

def normalize(x):
    y = normalizer.normalize_str(delete_punct(x))
    y = y.replace("\n", " ")
    # remove double spaces
    return re.sub(' +', ' ', y).strip()

