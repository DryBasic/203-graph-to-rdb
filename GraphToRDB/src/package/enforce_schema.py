from schema import Schema, Or, Optional, Use

data_node = Schema({
    "type": "node",
    "id": str,
    "labels": [str],
    "properties": {
        str: Or(str, int, float, list, dict)
    }
})
data_edge = Schema({
    "id": str,
    "type": "relationship",
    "label": str,
    "properties": {
        str: Or(str, int, float, list, dict)
    },
    "start": {
        "id": str,
        "labels": [str]
    },
    "end": {
        "id": str,
        "labels": [str]
    }
})

map_description = """Shape of mapping configuration dict.
Format is meant to be declarative of the desired output tables.
"""
map = Schema({
    'entity_tables': [{
        'map_node_label': [str],
        'table_name': str,
        'pkey': Optional(Or(int, str)), # maybe default to node_id?
        'columns': [{
            'name': str,
            'dtype': str, # should set options
            'map_node_property': str
        }]
    }],
    'relationship_tables': [{
        'map_edge_label': str,
        'table_name': str,
        'pkey': Optional(Or(int, str)), # maybe default to edge_id?
        'map_from_node_label': [str],
        'map_to_node_label': [str],
        'columns': [{
            'name': str,
            'dtype': str,
            'map_edge_property': str
        }]
    }]
}, description=map_description, name='Map Config (graph to relational)')