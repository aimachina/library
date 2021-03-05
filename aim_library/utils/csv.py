# pylint: disable=no-name-in-module
# pylint: disable=import-error
import os
import re
import csv
from multiprocessing import cpu_count, Pool

from nlp.stopwords import remove_stopwords
from utils.string import clean_string, title_to_snake, first_valid, remove_accents
from utils.common import batch_generator


URL_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-_~+#,%&=*;:@/"


def clean_headers(headers):
    headers = [clean_string(h) for h in headers]
    headers = [title_to_snake(h) for h in headers]
    return headers


def filter_empty(lines):
    return [line for line in lines if any(line)]


def line_remove_stopwords(line):
    return [remove_stopwords(cell.lower()) for cell in line]


def fix_spaces(text, charset='":;,.<>/?[]{}\\|"'):
    for char in charset:
        text = text.replace(" " + char, char).replace(char + " ", char)
    return text.strip()


def line_fix_spaces(lines):
    return [fix_spaces(l) for l in lines]


def parallel_clean_csv(dirty_path, clean_path):
    reader = csv.reader(open(dirty_path))
    writer = csv.writer(open(clean_path, "w"))
    headers = clean_headers(next(reader))
    writer.writerow(headers)
    with Pool(cpu_count()) as p:
        for batch in batch_generator(reader):
            new_lines = p.map(line_remove_stopwords, batch)
            new_lines = p.map(line_fix_spaces, new_lines)
            new_lines = filter_empty(new_lines)
            writer.writerows(new_lines)


def clean_csv(dirty_path, clean_path):
    reader = csv.reader(open(dirty_path))
    writer = csv.writer(open(clean_path, "w"))
    headers = clean_headers(next(reader))
    writer.writerow(headers)
    for line in reader:
        new_line = [fix_spaces(remove_stopwords(cell.lower())) for cell in line]
        if any(new_line):
            writer.writerow(new_line)


def find_urls(lines):
    urls = set()
    for line in lines:
        for text in line:
            text_urls = {url for url in url_generator(text)}
            urls = urls.union(text_urls)
    return urls


def url_generator(s):
    matches = list(re.finditer("http", s))
    matches.reverse()
    for match in matches:
        index = match.start()
        sufix = s[index:]
        s = s[:index]
        yield first_valid(sufix, URL_CHARS)

