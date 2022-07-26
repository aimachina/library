# pylint: disable=import-error

import unicodedata
import numpy as np
import re
from itertools import groupby
from fuzzywuzzy import process
from fuzzysearch import find_near_matches
from math import ceil
import unidecode
from polyleven import levenshtein

ALLOWED_CHARS = (
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ,./<>?:;\\ `~!@#$%^&*()[]{}_+-=|¥\n"
)


def remove_accents(input_str, fmt="NFKD"):
    nkfd_form = unicodedata.normalize(fmt, input_str)
    return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])


def clean_string(s, charset=ALLOWED_CHARS):
    if not isinstance(s, str):
        s = s.decode("utf-8")  # Ensure unicode
    s = remove_accents(s)
    s = "".join([c for c in s if c in charset])  # Only keep some special characters
    return s


def format_string(input_str):
    return remove_accents(input_str).upper()


def format_datetime_string(string):
    return string.upper().replace(".", "")


def title_to_snake(title):
    return title.lower().replace(" ", "_")


def first_valid(string, charset=ALLOWED_CHARS):
    for index, char in enumerate(string):
        if not char in charset:
            break
    return string[:index]


replace_dict = {
    "P": "0",
    "B": "1",
    "V": "1",
    "F": "2",
    "H": "2",
    "T": "3",
    "D": "3",
    "S": "4",
    "Z": "4",
    "C": "4",
    "X": "4",
    "Y": "5",
    "L": "5",
    "N": "6",
    "Ñ": "6",
    "M": "6",
    "Q": "7",
    "K": "7",
    "G": "8",
    "J": "8",
    "R": "9",
    "1": "A",
    "2": "B",
    "3": "C",
    "4": "D",
    "5": "E",
    "6": "F",
    "7": "G",
    "8": "H",
    "9": "I",
    "0": "J",
}


def phonetic_string(string):
    s = format_string(string)
    new_str = ""
    for c in s:
        new_str += replace_dict[c] if c in replace_dict else ""
    return new_str


def levenshtein_distance(seq1, seq2):
    size_x = len(seq1) + 1
    size_y = len(seq2) + 1
    matrix = np.zeros((size_x, size_y))
    for x in range(size_x):
        matrix[x, 0] = x
    for y in range(size_y):
        matrix[0, y] = y

    for x in range(1, size_x):
        for y in range(1, size_y):
            if seq1[x - 1] == seq2[y - 1]:
                matrix[x, y] = min(matrix[x - 1, y] + 1, matrix[x - 1, y - 1], matrix[x, y - 1] + 1)
            else:
                matrix[x, y] = min(matrix[x - 1, y] + 1, matrix[x - 1, y - 1] + 1, matrix[x, y - 1] + 1)
    return matrix[size_x - 1, size_y - 1]


def inners_levenshtein(query, candidate, threshold=5):
    diff = len(candidate) - len(query)
    length_score = abs(len(query.strip()) - len(candidate.strip()))
    if diff < 0:
        candidate += abs(diff) * " "
        diff = 0
    if diff == 0:
        edit_distance = levenshtein(query.upper(), candidate.upper(), threshold)
        return edit_distance, length_score, 0
    l = len(query)
    distances = []
    for i in range(diff):
        #print(candidate[i : l + i].upper())
        distances.append(levenshtein(query.upper(), candidate[i : l + i].upper(), threshold))
    min_distance = min(distances)
    min_index = distances.index(min_distance)
    return min_distance, length_score, min_index


def eval_fuzzywuzzy(query, candidate, threshold=0, ignore_case=False, ignore_special=False, use_lenvs=True):
    # compa_lvth = int(round((90 - threshold)/10))
    if ignore_case:
        query = query.lower()
        candidate = candidate.lower()
    if ignore_special:
        query = unidecode.unidecode(query)
        candidate = unidecode.unidecode(candidate)
    if use_lenvs:
        xquery = query.replace(" ", "")
        xcandidate = candidate.replace(" ", "")
        newthreshold = threshold if threshold < 100 else 100
        compa_lvth = round(round((100 - newthreshold) / 10))
        mind, _, mscr = inners_levenshtein(xquery, xcandidate, threshold=compa_lvth + 1)
        newscore = 100 - mind * 10
        if newscore >= threshold:
            return newscore, xcandidate[mscr:]
    if not (' ' in query):
        result = process.extractBests(query, candidate.split(), score_cutoff=threshold)
    else:
        result = process.extractBests(query, (candidate,), score_cutoff=threshold)
    inners_thres = ceil(((100 - threshold) / 100) * len(query))
    if result:
        score = 0
        v_string = ''
        for r in result:
            (x_string, initial_v) = r
            if find_near_matches(query, x_string, max_l_dist=inners_thres):
                if initial_v > score:
                    score = initial_v
                    v_string = x_string
    else:
        (v_string, score) = '', 0
    return float(score), v_string


class SpanishSoundex():

    def __init__(self, equivalent_letter_code_dict=None):
        self.translations = equivalent_letter_code_dict or \
                            dict(zip('A|E|I|O|U|Y|W|H|B|P|F|V|C|S|K|G|J|Q|X|Z|D|T|L|M|N|Ñ|R|LL|RR'.split('|'), \
                                     '00000500102174788744335666959'))
        self.pad = lambda code: '{}0000'.format(code)[:4]

    def phonetics(self, word: str) -> str:
        """
        Return the Soundex equivalent code of the word.
        """
        if not isinstance(word, str):
            raise ValueError('Expected a unicode string!')

        if not len(word):
            raise ValueError('The given string is empty.')

        word = word.upper()
        word = re.sub(r'[^A-Z]', r'', word)

        # Isolate repeated special words: LL, RR
        separate_LL_RR = re.split(r'(LL|RR)', word)
        pre_code = ''
        for group_letters in separate_LL_RR:
            if 'LL' in group_letters:
                pre_code = pre_code + self.translations['LL']
            elif 'RR' in group_letters:
                pre_code = pre_code + self.translations['RR']
            else:
                pre_code = pre_code + ''.join(self.translations[char] for char in group_letters)

        code = self._squeeze(pre_code).replace('0', '')
        return self.pad(code)

    def _squeeze(self, word: str) -> str:
        """Squeeze the given sequence by dropping consecutive duplicates."""
        return ''.join(x[0] for x in groupby(word))

    def sounds_like(self, word1: str, word2: str) -> bool:
        """Compare the phonetic representations of 2 words."""
        return self.phonetics(word1) == self.phonetics(word2)