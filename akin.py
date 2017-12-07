from collections import defaultdict
from collections import namedtuple
import csv
import json
import os
from datetime import datetime
from datasketch import MinHash, MinHashLSH, MinHashLSHEnsemble

class AkinSettings(object):
    def __init__(self, path_to_settings):
        self._config = None
        with open(path_to_settings) as f:
            self._config = json.load(f)

    @property
    def datasourcesettings(self):
        return self._config.get('data_sources', [])

class DataSource(object):
    def __init__(self, sourcesettings):
        self._csv_location = sourcesettings.get('source_file', None)
        self._indexer_settings = sourcesettings.get('indexer_settings', None)
        self._all_data_entries = []

    @property
    def indexer_settings(self):
        return self._indexer_settings

    @property
    def data(self):
        return self._all_data_entries

    def load(self):
        if self._csv_location:
            if os.path.exists(self._csv_location):
                self._load(self._csv_location)
                return True
            else:
                print('Source location "{}" does not exist'.format(self._csv_location))
                return False
        else:
            print('No source location specified')
            return False

    def _load(self, csv_location):
        with open(csv_location, 'r', encoding='utf8') as f:
            for row in csv.DictReader(f, dialect="excel"):
                self._all_data_entries.append(row)
        return True

class Akin(object):
    def __init__(self, path_to_settings):
        self._settings = AkinSettings(path_to_settings)
        self._data_sources = list()
        self._groups = list()

    @property
    def datasources(self):
        return self._data_sources

    GroupData = namedtuple('GroupData', ['data_source', 'lsh', 'field', 'values'])
    @property
    def groups(self):
        return self._groups

    def initialize(self):
        for data_source_config in self._settings.datasourcesettings:
            data_source = DataSource(data_source_config)
            data_source.load()
            self._data_sources.append(data_source)

        for data_source in self.datasources:
            self._index_data_source(data_source)

    def _index_data_source(self, data_source):
        for indexer_setting in data_source.indexer_settings:
            index_type = indexer_setting.get('index_type', 'minhashlsh')
            index_thresholds = indexer_setting.get('thresholds', [100])
            use_shingles = indexer_setting.get('use_shingles', False)
            shingle_len = indexer_setting.get('shingle_length', 3)
            num_perm = indexer_setting.get('num_permutations', 128)
            fields = indexer_setting.get('fields_to_index', [])
            for field_to_index in fields:
                for threshold in index_thresholds:
                    field_hash, lsh, groups = Akin._index_field(data_source, field_to_index, index_type,
                                                                False, threshold, num_perm,
                                                                use_shingles, shingle_len)
                    group_data = self.GroupData(data_source=data_source, lsh=lsh, field=field_hash, values=groups)
                    self.groups.append(group_data)

    @staticmethod
    def _index_field(data_source, field, index_type, force_rehash, threshold,
                     num_perm=128, use_shingles=False, shingle_len=4):
        shingle_marker = str(use_shingles) + (str(shingle_len) if use_shingles else '')
        field_hash = '_'.join([field, index_type, str(threshold), str(num_perm), shingle_marker])
        field_hash = '_' + field_hash # Use an underscore to denote that this is a hidden field
        field_hash_len = field_hash + '_len'
        lsh = MinHashLSH(threshold, num_perm)
        for i, entry in enumerate(data_source.data):
            if force_rehash or field_hash not in entry:
                min_hash = MinHash(num_perm)
                field_value = entry[field].lower()
                set_len = 0
                if use_shingles:
                    if len(field_value) > shingle_len:
                        for w in [field_value[i:i + shingle_len] for i in range(len(field_value) - shingle_len + 1)]:
                            min_hash.update(w.encode('utf8'))
                            set_len += 1
                else:
                    for w in field_value.split():
                        min_hash.update(w.encode('utf8'))
                        set_len += 1
                entry[field_hash] = min_hash
                entry[field_hash_len] = set_len
            lsh.insert(i, entry[field_hash])

        all_groups = list()
        unseen_indices = [1] * len(data_source.data)
        for i, entry in enumerate(data_source.data):
            if i % 100000 == 0:
                print(str(i))
            if unseen_indices[i] == 0 or entry[field_hash_len] == 0:
                continue
            group_cnts = defaultdict(list)
            group_cnts = list()
            matches = 0
            for j in lsh.query(entry[field_hash]):
                matches += 1
                group_cnts.append(data_source.data[j])
                unseen_indices[j] = 0
            if matches > 1:
                all_groups.append(group_cnts)

        return field_hash, lsh, all_groups

def export_group(group_data):
    filename = group_data.field + '.txt'
    with open(filename, 'w') as f:
        for lg in [g for g in group_data.values if len(g)>1]:
            f.write(str([[gv for gk, gv in g.items() if not gk.startswith('_')] for g in lg]) + '\n')
            #f.write(str([(g['ADVERTISER_NAME'],g['PRODUCT_NAME'],g['PRODUCT_ID']) for g in lg]) + '\n')

if __name__ == "__main__":
    test = Akin('brand_settings.json')
    test.initialize()
    print(test.datasources[0])