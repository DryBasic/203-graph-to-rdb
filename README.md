# GraphToRDB Demo

Please refer to `demo.ipynb` for an interactive demo of the graph transformer package.

### Getting Started
While the package is written with PyPI standards in mind, it was not submitted, so you cannot pip install it.

Download the GraphToRDB directory and reference it as an import within your project. Dependencies/requirements are listed in the `pyproject.toml`. They are also listed in the first cell of `demo.ipynb`.

The `example_map.yml` may be helpful to use as a template when writing your own configs. The `example_data.json` file represents the JSON exports generated by [neo4j](https://neo4j.com/product/neo4j-graph-database/). JSON that does not match this schema will raise exceptions.  