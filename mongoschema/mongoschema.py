from collections import defaultdict
from prettytable import PrettyTable


class Schema(object):

    "Gets the schema of a MongoDB collection"

    def __init__(self, collection, where_dict={}, limit=0):
        """
                Initializes Schema for a given collection

                :param collection: The collection instance
                :type  collection: pymongo.collection.Collection

                :param where_dict: Filters (specific fields/value ranges etc.)
                :type  where_dict: dictionary

                :param limit: Number of docs to be sampled
                :type  limit: int

        """
        self.collection = collection
        self.where_dict = where_dict
        self.limit = limit

    def get_pretty_table(self, key_type_count, total_docs):
        """
                Returns PrettyTable object built using the key_type dictionary

                :param key_type_count: The distribution of key types
                :type key_type_count: dictionary

                :return: PrettyTable built from the key type dict
                :rtype: PrettyTable object

        """
        pretty_table_headers = [
            'Key', 'Occurrence Count', 'Occurrence Percentage', 'Value Type', 'Value Type Percentage']
        result_table = PrettyTable(pretty_table_headers)

        for key, key_types in key_type_count.iteritems():
            total_keys = sum(key_types.values())
            max_key_type_count = max(key_types.values())

            max_key_type = [key_type for key_type, key_type_count in key_types.iteritems(
            ) if key_type_count == max_key_type_count][0]

            max_key_percent = round(
                max_key_type_count * 100.0 / total_keys, 2) if total_keys else 0.0
            occurrence_percent = round(total_keys * 100.0 / total_docs, 2) if total_docs else 0.0

            prettytable_row = [
                key, total_keys, occurrence_percent, max_key_type, max_key_percent]
            result_table.add_row(prettytable_row)

        return result_table

    def get_schema(self, return_dict=True):
        """
            Returns the schema related stats of a MongoDB collection

            :return: Total number of docs sampled, Dictionary containing the stats 
            :rtype: int, Dictionary object

        """
        total_docs = 0
        key_type_default_count = {
            int: 0,
            float: 0,
            str: 0,
            bool: 0,
            dict: 0,
            list: 0,
            set: 0,
            tuple: 0,
            None: 0,
            object: 0,
            unicode: 0,
            "other": 0,
        }

        mongo_collection_docs = self.collection.find(
            self.where_dict).limit(self.limit)

        key_type_count = defaultdict(lambda: dict(key_type_default_count))

        for doc in mongo_collection_docs:
            for key, value in doc.iteritems():
                if type(value) in key_type_count[key].keys():
                    key_type_count[key][type(value)] += 1
                else:
                    key_type_count[key]["other"] += 1
            total_docs += 1

        result_table = self.get_pretty_table(key_type_count, total_docs)

        if not return_dict:
            return total_docs, result_table
        return total_docs, key_type_count

    def print_schema(self):
        """
            Prints the schema related stats of a MongoDB collection
        """
        total_docs, result_table = self.get_schema(return_dict=False)

        print "Total number of docs : {total_docs}".format(total_docs=total_docs)
        print result_table
