from .enforce_schema import Schemas
from schema import SchemaError
from json import loads; from yaml import safe_load
from os import path


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
    * in-memory: access a dict of tables (tuple of tuples) from self.rdb
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
        # Additional validation needed:
        # input sanitation, no duplicate table names, etc.
        validation.map.validate(self.mapping)

        self._initialize_rdb()

        # Validating data
        with open(data) as f:
            for i, line in enumerate(f):
                element = loads(line)
                line_num = i+1
                self._validate_element(element, line_num)
                self.data.append(element)
                self._insert_into_rdb(element, begin_conversion)

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
            
    def _initialize_rdb(self) -> None:
        self.rdb = {}
        for entity in self.mapping['entity_tables']:
            header = ["id"]
            for column in entity['columns']:
                header.append(column["name"])
            self.rdb[entity['table_name']] = {'header': header, 'data': ()}

        for relation in self.mapping['relationship_tables']:
            header = ["id", "from_id", "to_id"]
            if relation.get("columns"):
                for column in entity['columns']:
                    header.append(column["name"])
            self.rdb[relation['table_name']] = {'header': header, 'data': ()}

    
    def _insert_into_rdb(self, object, do) -> None:
        if do:
            pass
    
    def generate_rdb(self) -> None:
        pass

    def write_to_csv(self, out_path: str) -> None:
        pass

    def generate_sql(self, write_to_directory=None) -> dict|None:
        """Writes PostgreSQL DDL for tables and insert statements.
        
        If no output path specified, will return dictionary with "CREATE" and "INSERT" keys. Otherwise generates .sql files.
        Does not write any constraints, keys, indexes, sequences, etc.
        For "list" data types, defaults to TEXT ARRAY type."""
        
        create = self._sql_create()
        insert = self._sql_insert()

        if write_to_directory:
            with open(path.join(write_to_directory, 'CREATE.sql'), 'w') as f:
                f.write(create)
        else:
            out = {}
            out['CREATE'] = create
            out['INSERT'] = insert
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

    def _sql_insert(self) -> dict:
        return None

    @staticmethod
    def _sql_dtype(type_str: str) -> str:
        types = {
            "string": "TEXT",
            "str": "TEXT",
            "int": "INTEGER",
            "integer": "INTEGER",
            "float": "NUMERIC",
            "list": "TEXT ARRAY"
        }
        return types.get(type_str)
    
    @staticmethod
    def _py_dtype(type_str: str) -> str:
        types = {
            "string": str,
            "str": str,
            "int": int,
            "integer": int,
            "float": float,
            "list": list
        }
        return types.get(type_str)