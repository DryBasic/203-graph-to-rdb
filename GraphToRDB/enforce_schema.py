from schema import Schema, Or, Optional, Use, SchemaError

class Schemas:
    """Collection of schema objects to be used for config and data validation"""
    def __init__(self) -> None:
        self.data_node = Schema({
            "type": "node",
            "id": str,
            "labels": [str],
            "properties": {
                str: Or(str, int, float, list, dict)
            }
        })
        self.data_edge = Schema({
            "type": "relationship",
            "id": str,
            "label": str,
            Optional("properties"): {
                str: Or(str, int, float, list, dict)
            },
            "start": {
                "id": str,
                "labels": [str],
                Optional("properties"): {
                    str: Or(str, int, float, list, dict)
                },
            },
            "end": {
                "id": str,
                "labels": [str],
                Optional("properties"): {
                    str: Or(str, int, float, list, dict)
                },
            }
        })

        map_description = """Shape of mapping configuration dict.
        Format is meant to be declarative of the desired output tables.
        """
        self.map = Schema({
            'entity_tables': [{
                'map_node_label': [str],
                'table_name': str,
                'columns': [{
                    'name': str,
                    'dtype': str, # should set options
                    'map_node_property': str
                }]
            }],
            'relationship_tables': [{
                'map_edge_label': str,
                'table_name': str,
                'map_from_node_label': [str],
                'map_to_node_label': [str],
                Optional('columns'): [{
                    'name': str,
                    'dtype': str,
                    'map_edge_property': str
                }]
            }]
        }, description=map_description, name='Map Config (graph to relational)')