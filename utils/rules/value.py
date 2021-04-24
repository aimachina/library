from utils.string import clean_string
from datetime import datetime
import re


class ItemValue:
    """
    Get values of desire items of a bimbo sale ticket and return its value or None
    """

    ALLOWED_CHARS_ALPHANUM = (
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
    )
    SIMILAR_NUMS = {"L": "1", "Z": "2", "A": "4", "S": "5", "G": "6", "T": "7", "B": "8", "O": "0", "/": "7"}

    def __init__(self):
        self.text_re = re.compile(r"[A-Za-z]+")

    def get_datetime(self, line: str):
        line_clean=clean_string(line,charset=self.ALLOWED_CHARS_ALPHANUM)
        date_items= re.findall(r"\w{,4}\d{1,8}",line_clean)
        date_value=date_items[0] if len(date_items)>=2 else ""
        if date_value.isdigit() and len(date_value)>=8:
            return datetime.strptime(date_items[0], '%d%m%Y')
        return None

    def add_dot_amount(self, amount: str) -> str:
        if amount.count(".") == 1:
            return amount

        no_dots = amount.replace(".", "")
        if len(no_dots) > 2:
            return f"{no_dots[:-2]}.{no_dots[-2:]}"

        return f"{no_dots}.00"

    def _translate_line(self,line:str)->str:
        similar_nums = str.maketrans(self.SIMILAR_NUMS)
        return line.translate(similar_nums)

    def get_only_numbers(self,clean_line:str)-> str:
        numbers=re.findall(r'(\d+\.?\d+?)', clean_line)
        if numbers:
            return numbers
        return None  

    def get_amount_format(self,line):
        
        value=clean_string(line,charset=self.ALLOWED_CHARS_ALPHANUM)
        amount=self.get_numbers(value)
        
        if not amount:
            return None 
        
        amount_str=str(amount[0])    
        insert_point=len(amount_str)-2
        return amount_str[:insert_point]+"."+amount_str[insert_point:] 


    def get_only_text(self,clean_line:str)->str:
        words = re.findall(self.text_re,clean_line)
        return words if len(words) else [""]
        
    def get_word_number(self,line:str)-> str:
        """
            GET FORMAT TEXT WORD NUMBER -> CAJA1, GERENTE12, USUARIO1234 
        """       
        words= re.findall(r"\w+\d{0,4}",line)
        return words

    def get_numbers(self,clean_line:str)->str:
        numbers = re.findall(r'\d+\.?\d{0,2}',clean_line)
        return "".join(str(n) for n in numbers) if len(numbers) else None

    def get_money_amounts(self,clean_line:str) -> str:
        numbers = re.findall(r'(?<=\$)\d+\.?\d{0,2}',clean_line)
        if not len(numbers):
            numbers = re.findall(r'\d+\.?\d{0,2}',clean_line)
        return numbers if len(numbers) else None


    def get_first_numbers(self,line:str)-> str:
        clean_line = self._translate_line(line)
        numbers=re.findall(r'(\d+)', clean_line)
        if numbers:
            for num in numbers:
                return num 
        return None        
    

    
    def get_date(self, line: str) -> str:
        """
        Get the date value of FECHA 26/11/2020 09:35:53
        """
    
        cleaned_line = clean_string(line.upper(), charset="0123456789 :-/+.")
        if line == "":
            return None

        date_values = {"year": 2020, "month": 1, "day": 1}
        date = re.split(r'-|/|\s|\+',cleaned_line)
        cleaned_line= self._translate_line(cleaned_line)
        time_items = re.split(r":|\.",cleaned_line)
        try:
            if len(date) >= 4 :
                year= re.findall(r"\d{4}|\d{2}",date[-2])[0]
                date_values.update({"year": int(year), "month": int(date[-2]), "day": int(date[-4]), "hour": int(time_items[-3]),"minute": int(time_items[-2]), "second": int(time_items[-1])})
            else :
                year= re.findall(r"\d{4}|\d{2}",date[-1])[0]
                date_values.update({"year": int(year), "month": int(date[-2]), "day": int(date[-3]), "hour": int(time_items[-3]),"minute" :int( time_items[-2]), "second": int(time_items[-1])})
                return datetime(**date_values)
        except ValueError:
            return None
        except IndexError:
            return None

        return None

