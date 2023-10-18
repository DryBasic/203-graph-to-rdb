from .enforce_schema import Schemas
from schema import SchemaError
import json

validation = Schemas()

class Transformer:
    """"""
    def __init__(self):
        self.data = []

    def load_jsonlines_file(self, file):
        with open(file) as f:
            for i, line in enumerate(f):
                element = json.loads(line)
                line_num = i+1
                self.validate_element(element, line_num)
                self.data.append(element)

    def load_json_list(self, jsonlist: list):
        for i, element in jsonlist:
            line_num = i+1
            self.validate_element(element, line_num)

    def validate_element(self, element, index):
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
            