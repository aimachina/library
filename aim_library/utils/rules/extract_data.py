from aim_library.utils.string import levenshtein_distance
from typing import List, Dict, Any

def inners_levenshtein(query, candidate):
    diff = len(candidate) - len(query)
    length_score = abs(len(query.strip()) - len(candidate.strip()))
    if diff < 0:
        candidate += abs(diff) * " "
        diff = 0
    if diff == 0:
        edit_distance = levenshtein_distance(query.upper(), candidate.upper())
        return edit_distance, length_score, 0
    l = len(query)
    distances = []
    for i in range(diff):
        distances.append(levenshtein_distance(query.upper(), candidate[i : l + i].upper()))
    min_distance = min(distances)
    min_index = distances.index(min_distance)
    return min_distance, length_score, min_index

def get_only_number_contain(dict_lines,caracter,find_word):
    if not dict_lines:
        return ""
    data=""
    keys=list(dict_lines.keys())
    next_word=False
    #print(f" find_word :{find_word} ")
    for k in keys[1:]:
        line_data=dict_lines.get(k,"")   
        min_distance,length_score, min_index = inners_levenshtein(find_word,line_data)
        data=get_number_caracter(line_data,caracter)
        #print(f" min_distance:{min_distance} line_data:{line_data} len {len(data)} ")
        if next_word:
             return data
        if min_distance <=3 and len(data)>0:
             return data
        elif min_distance<=3 and len(data)==0:
             next_word=True
    return data    
    
def numextract(text):
    return "".join([s for s in list(text) if s.isdigit()])

def numextract_float(text):
    return "".join([s for s in list(text) if (s.isdigit() or s=="." )])


def get_number_caracter(text,caracter):
    return "".join([s for s in list(text) if (s.isdigit() or s==caracter)])

def get_format_integer(text):
    try:
    	return "{:.0f}".format(float(text))
    except:
    	return text
    return text	   

def get_format_total(total_data):
    count_point=total_data.count(".")
    return total_data.replace(".",",",count_point-1)

def getFirstNumber(words:list,idx_begin:int):
    if len(words)<=idx_begin:
        return "",idx_begin
    for word in words[idx_begin:-1]:
        numbers = numextract(word)
        if numbers.isnumeric():
            number= word  
            idx_begin+=1
            return number,idx_begin
    return "",idx_begin

def get_words_in_line(document:Dict,max_lines:int):
    words=[]
    if document:
        for page in  document.get("pages",[]):
            for area in page.get("areas",[]):
                for paragraph in area.get("paragraphs",[]):
                    for idx,line in enumerate(paragraph.get("lines",[])):
                        if max_lines>0:
                            if idx>max_lines:
                                break
                        words.extend(line.get("words",[]))
                            
    return words

def get_lines_document(document:Dict,max_lines:int):
    lines=[]
    if document:
        for page in  document.get("pages",[]):
            for area in page.get("areas",[]):
                for paragraph in area.get("paragraphs",[]):
                    for idx,line in enumerate(paragraph.get("lines",[])):
                        if max_lines>0:
                            if idx>max_lines:
                                break
                        lines.append(line)
                            
    return lines
