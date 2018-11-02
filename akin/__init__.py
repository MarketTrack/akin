from typing import Dict, Iterable, NamedTuple, Tuple
import sqlite3
import pickle
from datasketch import MinHash, MinHashLSH
from logging import getLogger

__version__ = '0.1'
_log = getLogger(__name__)


class AkinSettings(object):
    def __init__(self, path_to_settings):
        from yaml import safe_load
        self._config = None
        with open(path_to_settings) as settings_file:
            self._config = safe_load(settings_file)

    @property
    def dblocation(self):
        return self._config.get('db_location', 'akin.db')


class DataSource(object):
    def __init__(self, name, data):
        self._name = name
        self._all_data_entries = data
        self._groups = dict()

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._all_data_entries

    @property
    def groups(self):
        return self._groups


class GroupData(NamedTuple):
    data_source: DataSource
    field: str
    lsh: MinHashLSH
    values: str


class GroupTemplate(NamedTuple):
    name: str
    description: str
    index_type: str
    threshold: float
    case_sensitive: int # as boolean
    use_shingles: int # as boolean
    shingle_length: int
    num_permutations: int


class Akin(object):
    def __init__(self, path_to_settings):
        self._settings = AkinSettings(path_to_settings)
        self._datasources = dict()
        self._grouptemplates = dict()

    @property
    def datasources(self):
        return self._datasources

    @property
    def grouptemplates(self):
        return self._grouptemplates

    def group_data(self, data_source: DataSource, field_to_index: str, group_settings: GroupTemplate):
        field_hash = Akin._generate_groupid(field_to_index, group_settings)

        # check if this group has already been created
        existing_group = [g for g in data_source.groups.values() if g.field == field_hash]
        if existing_group:
            return existing_group[0]

        field_hash, lsh, groups = Akin._index_field(data_source, field_to_index, group_settings)

        group_data = GroupData(data_source=data_source.name, lsh=lsh, field=field_hash, values=groups)
        data_source.groups[group_data.field] = group_data

        db_cursor, conn = self._get_db_cursor()
        db_cursor.execute('''INSERT INTO group_values VALUES (?,?,?,?)''', \
                          (data_source.name, field_hash, pickle.dumps(lsh), pickle.dumps(groups)))
        conn.commit()
        conn.close()
        return groups

    def add_grouptemplate(self, group_template: GroupTemplate) -> Tuple[bool, str]:
        if self.grouptemplates.get(group_template.name):
            return False, f'Data source with the name "{group_template.name}" already exists'
        db_cursor, conn = self._get_db_cursor()
        db_cursor.execute('''INSERT INTO group_templates VALUES (?,?,?,?,?,?,?,?)''', \
                          (group_template.name, group_template.description, group_template.index_type, group_template.threshold,
                           group_template.case_sensitive, group_template.use_shingles, group_template.shingle_length, 
                           group_template.num_permutations))
        conn.commit()
        conn.close()
        return True, 'Successfully added group template'

    def add_datasource(self, data_source_name: str, data: Iterable[Dict[str, str]]) -> Tuple[bool, str]:
        if self.datasources.get(data_source_name):
            return False, f'Data source with the name "{data_source_name}" already exists'
        self.datasources[data_source_name] = DataSource(data_source_name, data)
        db_cursor, conn = self._get_db_cursor()
        db_cursor.execute('''INSERT INTO data_sources VALUES (?,?)''', \
                          (data_source_name, pickle.dumps(data)))
        conn.commit()
        conn.close()
        return True, 'Successfully parsed and uploaded file'

    def delete_datasource(self, data_source_name: str) -> Tuple[bool, str]:
        try:
            del self.datasources[data_source_name]
        except KeyError:
            return False, f'No data source with the name "{data_source_name}" exists'
        db_cursor, conn = self._get_db_cursor()
        db_cursor.execute('''DELETE FROM data_sources WHERE data_source_name=?''', \
                          (data_source_name,))
        db_cursor.execute('''DELETE FROM group_values WHERE data_source_name=?''', \
                          (data_source_name,))
        conn.commit()
        conn.close()
        return True, 'Successfully deleted datasource'

    def _get_db_cursor(self) -> Tuple[sqlite3.Cursor, sqlite3.Connection]:
        db_file = self._settings.dblocation
        connection = sqlite3.connect(db_file)
        return connection.cursor(), connection

    def initialize(self):
        db_cursor, conn = self._get_db_cursor()

        # Shrink the database
        db_cursor.execute('''VACUUM''')

        # Ensure that the necessary tables exist
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS data_sources (data_source_name text PRIMARY KEY, data blob);''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS group_values (data_source_name text, group_name text, lsh blob, group_values blob,
                            PRIMARY KEY(data_source_name,group_name));''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS group_templates (template_name text, description text, index_type type, threshold real,
                            case_sensitive integer, use_shingles integer, shingle_length integer, num_permutations integer,
                            PRIMARY KEY(template_name));''')

        conn.commit()

        for data_source_name, data_raw in db_cursor.execute('''SELECT data_source_name, data FROM data_sources'''):
            data = pickle.loads(data_raw)
            db_ds = DataSource(data_source_name, data)
            self.datasources[data_source_name] = db_ds

        for data_source_name, group_name, lsh_raw, group_values_raw in db_cursor.execute('''SELECT data_source_name, group_name, lsh, group_values FROM group_values'''):
            group_values = pickle.loads(group_values_raw)
            lsh = pickle.loads(lsh_raw)
            loaded_group = GroupData(data_source=data_source_name, field=group_name, lsh=lsh, values=group_values)
            if data_source_name in self.datasources:
                self.datasources[data_source_name].groups[loaded_group.field] = loaded_group
            else:
                _log.warn('Initialize skipped unknown datasource "%s".', data_source_name)

        for template_name, description, index_type, threshold, case_sensitive, use_shingles, shingle_length, num_permutations in db_cursor.execute('''SELECT * FROM group_templates'''):
            self._grouptemplates[template_name] = GroupTemplate(name=template_name, description=description, index_type=index_type,
                                                                threshold=threshold, case_sensitive=case_sensitive, use_shingles=use_shingles, 
                                                                shingle_length=shingle_length, num_permutations=num_permutations)
        if not self._grouptemplates:
            default_grouptemplate = GroupTemplate(name="Default", description="Use MinHashLSH to find groups with a Jaccard similarity of 1.",
                                                  index_type="minhashlsh", threshold=1.0, case_sensitive=0, use_shingles=0, shingle_length=4,
                                                  num_permutations=128)
            default_grouptemplate_90 = GroupTemplate(name="Default 0.9", description="Use MinHashLSH to find groups with a Jaccard similarity of 0.9.",
                                                  index_type="minhashlsh", threshold=0.9, case_sensitive=0, use_shingles=0, shingle_length=4,
                                                  num_permutations=128)
            default_grouptemplate_95_s = GroupTemplate(name="Default 0.95 Shingled 3", description="Use MinHashLSH with shingling to find groups with a Jaccard similarity of 0.95.",
                                                  index_type="minhashlsh", threshold=0.95, case_sensitive=0, use_shingles=1, shingle_length=3,
                                                  num_permutations=128)
            self.add_grouptemplate(default_grouptemplate)
            self.add_grouptemplate(default_grouptemplate_90)
            self.add_grouptemplate(default_grouptemplate_95_s)

        conn.close()

    @staticmethod
    def _generate_groupid(field, group_settings: GroupTemplate):
        index_type = group_settings.index_type
        threshold = group_settings.threshold
        use_shingles = group_settings.use_shingles
        shingle_len = group_settings.shingle_length
        num_perm = group_settings.num_permutations

        shingle_marker = str(use_shingles) + (str(shingle_len) if use_shingles else '')
        field_hash = '_'.join([field, index_type, str(threshold), str(num_perm), shingle_marker])
        field_hash = '__' + field_hash # Use a double underscore to denote that this is a system field
        return field_hash

    @staticmethod
    def _index_field(data_source, field, group_settings: GroupTemplate):
        force_rehash = False

        index_type = group_settings.index_type
        threshold = group_settings.threshold
        use_shingles = group_settings.use_shingles
        shingle_len = group_settings.shingle_length
        num_perm = group_settings.num_permutations

        if index_type != 'minhashlsh':
            raise NotImplementedError(f'index type "{index_type}" is not implemented')

        field_hash = Akin._generate_groupid(field, group_settings)
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
            if unseen_indices[i] == 0 or entry[field_hash_len] == 0:
                continue
            potential_group = list()
            for j in lsh.query(entry[field_hash]):
                potential_group.append(data_source.data[j])
                unseen_indices[j] = 0
            if len(potential_group) > 1:
                all_groups.append(potential_group)

        return field_hash, lsh, all_groups


def export_group(group_data: GroupData):
    filename = group_data.field + '.txt'
    with open(filename, 'w') as f:
        for lg in [g for g in group_data.values if len(g)>1]:
            f.write(str([[gv for gk, gv in g.items() if not gk.startswith('_')] for g in lg]) + '\n')


if __name__ == "__main__":
    akin = Akin('brand_settings.yml')
    akin.initialize()
