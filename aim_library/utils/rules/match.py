from aim_library.utils.string import levenshtein_distance
from functools import reduce

def get_porcentage_coincidence(base_str: str, to_compare: str) -> float:
    """
    Return the porcentage of coincidence between two strings if are not empty or None
    """
    if not (base_str and to_compare):
        return 0.0

    base_str = base_str.translate(str.maketrans({" ": ""}))
    to_compare = to_compare.translate(str.maketrans({" ": ""}))
    if not (base_str or to_compare):
        return 0.0

    if base_str == to_compare:
        return 100

    distance = levenshtein_distance(base_str, to_compare)

    result = (1 - (distance / len(base_str))) * 100
    return 0.0 if result < 0 else round(result, 3)


def get_matches_coincidences(user_access,repo,search_coincidence):
    repo.set_collection(search_coincidence["source"]["collection"])
    report = repo.get_one_by_filters(user_access=user_access,filters=search_coincidence["source"]["filters"])

    repo.set_collection(search_coincidence["target"]["collection"])
    document = repo.get_one_by_filters(user_access=user_access,filters = search_coincidence["target"]["filters"])

    coincidence= 0.0
    if not document or not report:
        return coincidence
    
    for fields in search_coincidence["fields_to_compare"]:
        report_field = str(reduce(dict.get,fields["source"],report))
        
        ticket_field = str(reduce(dict.get,fields["target"],document))
        coincidence += get_porcentage_coincidence(report_field,ticket_field)
        
    return coincidence / len(search_coincidence["fields_to_compare"])