from utils.string import levenshtein_distance, clean_string, SpanishSoundex
import re
from utils.rules.model import Line


class Identifier():
    """
    Identifier of desire items of a chedraui sale ticket and return its value or None
    """
    def __init__(self):
        super().__init__()

    
    def get_line_by_label(self,line:Line,sorted_lines:list,label:str,item_value):
        if not label:
            label=""
        while line.uuid != "-1":
            clean_line=self.clean_line_label(line.text)
            if (
                label in line.text
                or label in clean_line
                or self.is_exit_label(
                        clean_line, label
                    )
                ):
                break
            line = item_value.get_next_line(sorted_lines)  
        return line         

    
    def _remove_spaces_and_to_lower(self, line: str) -> str:
        return line.replace(" ", "").lower()


    def is_similar_word(self, word_in_doc: str, first_desire_word: str) -> bool:
        """
        Check if s is first_desire_word or something similar
        """
        #first_letters = s[: len(first_desire_word)]
        word_in_doc = self.clean_line(word_in_doc)
        
        if(not word_in_doc or not self._is_valid_line(word_in_doc)):
            return False
        
        
        return self._is_it_similar(word_in_doc, first_desire_word) or self._is_it_phonetic_similar(word_in_doc, first_desire_word)
    
    def is_exit_label(self, line: str,label:str) -> str:
        """
        Search word like as first word
        """
        clean_line = self.is_first_word(
            line=line, first_word_check=label, charset="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:"
        )
        if clean_line == None:
            return None

        return clean_line
    """
    Rules that could be use in many documents
    """

    def _is_valid_line(self, line: str) -> bool:
        return line != None and isinstance(line, str) and len(line.replace(" ", "")) > 0

    def _is_it_similar(self, to_compare: str, base: str) -> bool:
        return levenshtein_distance(to_compare, base) < (len(base) // 2)

    def _is_it_phonetic_similar(self, to_compare: str, base: str) -> bool:
        return SpanishSoundex().sounds_like(to_compare, base)

    def _is_words_and_pay_line(self, first_desire_word: str, line: str) -> bool:
        cleaned_line = clean_string(
            self._remove_spaces_and_to_lower(line), charset="abcdefghijklmnopqrstuvwxyz0123456789."
        )

        # Does have the right body?
        if (
            (not (re.search(r"[a-z]+\d+\.\d{2}", cleaned_line) or re.search(r"[a-z]+\d{3,}", cleaned_line)))
            or re.search(r"^[a-z]+$", cleaned_line)
            or re.search(r"^\d+$", cleaned_line)
            or re.search(r"^\d+\.\d+$", cleaned_line)
        ):
            return False

        # Try to guess if first chars are: first_desire_word
        firs_input_word = ""
        # Does it has dot?
        if "." not in cleaned_line:
            firs_input_word = re.split(r"\d{3,}", cleaned_line)[0]
        else:
            firs_input_word = re.split(r"\d+\.\d{2}", cleaned_line)[0]
        if not firs_input_word:
            return False

        return self._is_it_similar(firs_input_word, first_desire_word) or self._is_it_phonetic_similar(
            firs_input_word, first_desire_word
        )

    def clean_line(self, line: str, charset: str = None) -> str:
        """
        Remove not desire chars from a string
        """
        return clean_string(
            line.upper(),
            charset=charset or "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        )

    def clean_line_label(self, line: str, charset: str = None) -> str:
        """
        Remove not desire chars from a string
        """
        return clean_string(
            line.upper(),
            charset=charset or "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
        )

    def is_first_word(self, line: str, first_word_check: str, charset: str = None) -> str:
        """
        Decide if the line start with first_word_check.If it does, returns a cleaned line
        based on charset
        """
        if not self._is_valid_line(line):
            return None

        len_word_check = len(first_word_check)
        cleaned_line = self.clean_line(line, charset=charset)

        if not (
            cleaned_line
            and (
                self._is_it_similar(cleaned_line[:len_word_check], first_word_check)
                or self._is_it_phonetic_similar(cleaned_line[:len_word_check], first_word_check)
            )
        ):
            return None

        return cleaned_line
