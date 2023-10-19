from .enforce_schema import Schemas
from schema import SchemaError
from json import loads; from yaml import safe_load; import csv
from os import path
from warnings import warn

validation = Schemas()

class Transformer:
    """
    Takes
    * data: path to your valid JSON lines file
    * mapping: path to your mapping config YAML or an in-memory dict
    * begin_conversion: default behavior generates RDB format during validation for efficiency. Alternatively, call generate_rdb() after construction.

    Key Constructor Operations
    1. Validates the schema of mappings on construction, will raise error during failure.
    2. Validates data object by object, writing to RDB format for each success (if begin_conversion), will raise error during failure.

    Attributes
    * data: the loaded and validated JSON lines file (list of dicts)
    * mapping: the loaded config (dict)
    * rdb: a dict containing the output relational data

    Output Options
    * in-memory: access a dict of tables (list of lists) from self.rdb
    * csv: call write_to_csv() method
    * sql statements: call generate_sql() method
    """
    def __init__(self, data: str, mapping: dict|str, begin_conversion: bool=True) -> None:
        self.data = []
        self.mapping = None

        # Validating mapping
        if isinstance(mapping, dict):
            self.mapping = mapping
        elif isinstance(mapping, str):
            with open(mapping) as f:
                self.mapping = safe_load(f)
        # Additional validation + exception handling needed:
        # input sanitation, no dup table names, no dup col names,
        # reserved colnames of "id", "from_id", "to_id"
        validation.map.validate(self.mapping)

        self._initialize_rdb()

        # Validating data 
        # additional val needed:
        # For node labels all members must be unique
        with open(data) as f:
            for i, line in enumerate(f):
                element = loads(line)
                line_num = i+1
                self._validate_element(element, line_num)
                self.data.append(element)
                self._insert_into_rdb(element, do=begin_conversion)

    def _initialize_rdb(self) -> None:
        """Create shape of self.rdb and create new hashes for data types and node/edge maps from self.mapping to simplify data conversion/insertion.
        """
        self.rdb = {}
        self._rdb_map_helper = {}
        self._rdb_label_to_table = {}
        for entity in self.mapping['entity_tables']:
            tname = entity["table_name"]
            # convert node label list to frozenset for a hashable collection
            # that allows for content comparison (nodes can have many unique labels)
            map_key = frozenset(entity["map_node_label"])
            self._rdb_label_to_table[map_key] = tname
            self._rdb_map_helper[map_key] = {}
            this_helper = self._rdb_map_helper[map_key]

            header = ["id"]
            for column in entity['columns']:
                cname = column["name"]
                header.append(cname)

                this_helper[cname] = {
                    "property": column["map_node_property"],
                    "dtype": self._py_dtype(column["dtype"])
                }
            self.rdb[tname] = {'header': header, 'data': []}

        # this code is kind of confusing due to copy paste nature
        # but with entity and relation swapped
        # probably a cleaner pattern available
        for relation in self.mapping['relationship_tables']:
            tname = relation["table_name"]
            map_key = relation["map_edge_label"]
            self._rdb_label_to_table[map_key] = tname
            self._rdb_map_helper[map_key] = {}
            this_helper = self._rdb_map_helper[relation["map_edge_label"]]

            header = ["id", "from_id", "to_id"]
            if relation.get("columns"):
                for column in relation['columns']:
                    cname = column["name"]
                    header.append(cname)

                    this_helper[cname] = {
                        "property": column["map_edge_property"],
                        "dtype": self._py_dtype(column["dtype"])
                    }
            self.rdb[tname] = {'header': header, 'data': []}

    def _validate_element(self, element: dict, index: int) -> None:
        match element.get('type'):
            case 'node':
                try:
                    validation.data_node.validate(element)
                except Exception as msg:
                    raise SchemaError(f'Error in line {index}: {msg}')
            case 'relationship':
                try:
                    validation.data_edge.validate(element)
                except Exception as msg:
                    raise SchemaError(f'Error in line {index}: {msg}')   
            case _:
                raise SchemaError(f'key "type" not found in line {index}, invalid JSON schema')
            

    def generate_rdb(self) -> None:
        self._initialize_rdb()
        for element in self.data:
            self._insert_into_rdb(element, do=True)
    
    def _insert_into_rdb(self, element, do) -> None:
        if do:
            match element["type"]:
                case "node":
                    key = frozenset(element["labels"])
                case "relationship":
                    key = element["label"]
            
            table = self._rdb_label_to_table.get(key)
            if table:
                record = [int(element["id"])]
                if element["type"] == "relationship":
                    record.append(int(element["start"]["id"]))
                    record.append(int(element["end"]["id"]))

                columns = self._rdb_map_helper[key]
                for col in columns:
                    property = columns[col]["property"]
                    type_func = columns[col]["dtype"]
                    if value := element["properties"].get(property):
                        record.append(type_func(value))
                    else:
                        record.append(None) # should be same as appending that value

                self.rdb[table]["data"].append(record)

            else:
                warn(f'"{key}" not in mapping. "{element["type"]}" of id {element["id"]} will be ignored')

    def write_to_csv(self, write_to_directory: str) -> None:
        for table in self.rdb:
            with open(path.join(write_to_directory, f"{table}.csv"), 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.rdb[table]["header"])
                writer.writerows(self.rdb[table]["data"])

    def generate_sql(self, write_to_directory=None) -> dict|None:
        """Writes PostgreSQL CREATE statements for tables.
        
        If no output path specified, will return dictionary with "CREATE" key. Otherwise generates .sql files.
        Does not write any constraints, keys, indexes, sequences, etc.
        For "list" data types, defaults to TEXT ARRAY type."""
        
        create = self._sql_create()
        # insert = self._sql_insert()

        if write_to_directory:
            with open(path.join(write_to_directory, 'CREATE.sql'), 'w') as f:
                f.write(create)
        else:
            out = {}
            out['CREATE'] = create
            # out['INSERT'] = insert
            return out        

    def _sql_create(self) -> str:
        creates = []
        for table in self.mapping['entity_tables']:
            top = f"CREATE TABLE {table['table_name']} (\n"
            cols = []
            for col in table['columns']:
                dtype = self._sql_dtype(col['dtype'])
                cols.append(f"\t{col['name']}\t\t{dtype}")
            bottom = ',\n'.join(cols) + "\n);"
            creates.append(str(top+bottom))

        for table in self.mapping['relationship_tables']:
            top = f"CREATE TABLE {table['table_name']} (\n"
            cols = []
            cols.append("\tfrom_id\t\tINTEGER")
            cols.append("\tto_id\t\tINTEGER")
            if table.get('columns'):
                for col in table['columns']:
                    dtype = self._sql_dtype(col['dtype'])
                    cols.append(f"\t{col['name']}\t\t{dtype}")
            bottom = ',\n'.join(cols) + "\n);"
            creates.append(str(top+bottom))

        return '\n\n'.join(creates)

    # NOT IMPLEMENTED
    # def _sql_insert(self) -> dict:
    #     statements = []
    #     for table in self.rdb:
    #         top = f'INSERT INTO {table} ({", ".join(self.rdb[table]["header"])}) VALUES (\n'
    #         bottom = []
    #         for record in self.rdb[table]["data"]:
    #             vals = ''
    #             for value in enumerate(record):
    #                 pass

    @staticmethod
    def _sql_dtype(type_str: str) -> str:
        type_str = type_str.lower()
        types = {
            "text":     "TEXT",
            "string":   "TEXT",
            "str":      "TEXT",
            "int":      "INTEGER",
            "integer":  "INTEGER",
            "float":    "NUMERIC",
            "numeric":  "NUMERIC",
            "list":     "TEXT ARRAY",
            "array":    "TEXT ARRAY"
        }
        return types.get(type_str)
    
    @staticmethod
    def _py_dtype(type_str: str) -> str:
        type_str = type_str.lower()
        types = {
            "text":     lambda x: str(x),
            "string":   lambda x: str(x),
            "str":      lambda x: str(x),
            "int":      lambda x: int(x),
            "integer":  lambda x: int(x),
            "float":    lambda x: float(x),
            "numeric":  lambda x: float(x),
            "list":     lambda x: list(x) if not isinstance(x, list) else x,
            "array":    lambda x: list(x) if not isinstance(x, list) else x
        }
        return types.get(type_str)