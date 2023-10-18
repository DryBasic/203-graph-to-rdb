from .enforce_schema import Schemas
from schema import SchemaError
import json
from os import path
import yaml

validation = Schemas()

class Transformer:
    """
    Takes
    * data: path to your datafile OR an already in-memory list of dicts (JSON objects)
    * mapping: path to your map_config.yml OR an already in-memory dict

    Validate the schema of the data and mappings, will raise error during failure.
    If successful, access the output through the "rdb" attribute.
    You may write these sets to csvs or as SQL CREATE & INSERT statements.
    """
    def __init__(self, data: str, mapping: dict|str) -> None:
        self.data = []
        self.mapping = None
        self.rdb = {}

        # could use exception handling
        if isinstance(mapping, dict):
            self.mapping = mapping
        elif isinstance(mapping, str):
            with open(mapping) as f:
                self.mapping = yaml.safe_load(f)
        validation.map.validate(self.mapping)

        with open(data) as f:
            for i, line in enumerate(f):
                element = json.loads(line)
                line_num = i+1
                self._validate_element(element, line_num)
                self.data.append(element)

    def _validate_element(self, element, index):
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