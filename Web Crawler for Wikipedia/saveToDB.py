# -------------------------------------------------------------------------------
# Name:        saveToDB
# Purpose:     will save the defauldict to a MongoDB Database
#
# Author:      Animesh Pandey
#
# Created:     12/03/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

client = MongoClient('mongodb://localhost:27017/')
db = client['wiki_data2']
ol_table = db['outlink_data']
il_table = db['inlink_data']
map_table = db['url_hash']


class inLinkDb(object):
    def update(self, data, curr_url_hash):
        if not il_table.find_one({"_id": data['_id'], \
                                  "inlinks": {"$elemMatch": {curr_url_hash: {"$exists": True}}}}):
            if data['_id'] == curr_url_hash:
                try:
                    il_table.insert(data)
                except DuplicateKeyError:
                    pass
            else:
                il_table.update({"_id": data['_id']}, \
                                {"$inc": {"inlink_count": 1}, "$push": {"inlinks": {curr_url_hash: 1}}}, True)
        else:
            il_table.update({"_id": data['_id'], \
                             "inlinks": {"$elemMatch": {curr_url_hash: {"$exists": True}}}}, \
                            {"$inc": {'inlink_count': 1}, "$inc": {"inlinks.$." + curr_url_hash: 1}})

    def disp(self):
        return [value for value in il_table.find()]

    def get_inlink_count(self, data):
        return il_table.find_one({'_id': data['_id']})['inlink_count']

    def clear(self):
        il_table.drop()

    def size(self):
        return il_table.count()


class urlHashDb(object):
    def update(self, data):
        if not map_table.find_one({'_id': data['_id']}):
            try:
                map_table.insert(data)
            except DuplicateKeyError:
                pass
        else:
            pass

    def disp(self):
        return [value for value in map_table.find()]

    def clear(self):
        map_table.drop()

    def size(self):
        return map_table.count()


class outLinkDb(object):
    def update(self, data):
        if not ol_table.find_one({'_id': data['_id']}):
            ol_table.insert(data)
        else:
            pass

    def disp(self):
        return [value for value in ol_table.find()]

    def size(self):
        return ol_table.count()

    def clear(self):
        ol_table.drop()
