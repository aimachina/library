import datetime


class MongoDAO:
    def __init__(self, collection):
        self._collection = collection

    def count(self, filters):
        return self._collection.count_documents(filters)

    def get_all(self, skip=0, limit=0, sort_by=None, order=-1):
        if not sort_by:
            result_set = self._collection.find().skip(skip).limit(limit)
        else:
            result_set = self._collection.find().skip(skip).limit(limit).sort(sort_by, order)
        return result_set

    def get(self, _id):
        filters = {"_id": _id}
        return self.get_first(filters)

    def get_many(self, ids):
        filters = {"_id": {"$in": ids}}
        return self.get_many_by(filters)

    def get_many_by(self, filters, skip=0, limit=0, sort_by=None, order=-1):
        if not sort_by:
            result_set = self._collection.find(filters).skip(skip).limit(limit)

        else:
            result_set = self._collection.find(filters).skip(skip).limit(limit).sort(sort_by, order)
        return result_set

    def get_many_uuids_by(self, filters, skip=0, limit=0, sort_by=None, order=-1):
        if not sort_by:
            result_set = self._collection.find(filters, {"uuid": 1}).skip(skip).limit(limit)

        else:
            result_set = (
                self._collection.find(filters, {"uuid": 1}).skip(skip).limit(limit).sort(sort_by, order)
            )
        return result_set

    def make_filters(self, **kwargs):
        filters = {}
        for k, v in kwargs.items():
            if isinstance(v, list):
                filters[k] = {"$in": v}
            else:
                filters[k] = v
        return filters

    def get_first(self, filters):
        return self._collection.find_one(filters)

    def get_first_exclude_fields(self, filters, exclude_fields):
        return self._collection.find_one(filters, exclude_fields)

    def delete_one(self, _id):
        r = self._collection.delete_one({"_id": _id})
        return r.deleted_count

    def delete_many(self, ids):
        r = self._collection.delete_many({"_id": {"$in": ids}})
        return r.deleted_count

    def delete_by(self, filters):
        r = self._collection.delete_many(filters)
        return r.deleted_count

    def save_one(self, item):
        from pymongo.errors import DuplicateKeyError

        if "_id" in item and not item["_id"]:
            item.pop("_id")
        try:
            r = self._collection.insert_one(item)
            return r.inserted_id
        except DuplicateKeyError as e:
            print(e)
            return None

    def sample(self, filters={}, n=1):
        pipeline = [{"$match": filters}, {"$sample": {"size": n}}]
        return self._collection.aggregate(pipeline)

    def save_many(self, items):
        for item in items:
            if "_id" in item and not item["_id"]:
                item.pop("_id")
        r = self._collection.insert_many(items)
        return r.inserted_ids

    def insert_many(self, items, ordered = True):
        r = self._collection.insert_many(items,ordered)
        return r.inserted_ids

    def update_one(self, _id, data):
        r = self._collection.update_one({"_id": _id}, {"$set": data}, upsert=False)
        return r.modified_count

    def update_or_insert(self, filters, data):
        return self._collection.find_one_and_update(
            filters, {"$set": data}, upsert=True, return_document=True
        )

    def update_by(self, filters, data, upsert=False):
        r = self._collection.update_one(filters, {"$set": data}, upsert=upsert)
        return r.modified_count

    def append_many_to_tag(self, filters: dict, tag: str, data_to_append: dict, upsert=False):
        return self._collection.find_one_and_update(
            filters,
            {"$addToSet": {tag: {"$each": data_to_append}}},
            upsert=upsert,
            return_document=True,
        )

    def update_push(self,filters: dict,push:dict):
        return self._collection.find_one_and_update(
            filters, 
            {"$push":push}
        )

    def update_many(self, filters, set_data,upsert=False):
        return self._collection.update_many(filters,{"$set": set_data},upsert=upsert)

    def drop_indexes(self):
        return self._collection.drop_indexes()
        
    def create_index(self, on_field, unique=True,sparse = False):
        return self._collection.create_index(on_field, unique=unique,sparse= sparse)

    def create_indexes(self, indexes):
        return self._collection.create_indexes(indexes)

    def make_list_emptyness_filter(self, is_empty=True):
        if is_empty:
            return {"$size": 0}
        return {"$not": {"$size": 0}}

    def list_field_is_empty(self, field, filters):
        filters[field] = self.make_list_emptyness_filter()
        return bool(self._collection.count_documents(filters))

    def make_daterange_filter(self, field, start_date=None, end_date=None):
        end_date = end_date or datetime.datetime.now()
        start_date = start_date or datetime.datetime.fromtimestamp(0)
        return {field: {"$gte": start_date, "$lte": end_date}}

    def get_aggregate(self, filters):
        return self._collection.aggregate(filters)

    def get_distinct(self, field: str, filters: dict):
        return self._collection.distinct(field, filters)
