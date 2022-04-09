# mongo-postprocessing

A dynamic scripting for in place data processing on MongoDB collections with generalized postprocessing pipelines aimed to handle any types and sizes of data.

The types of processing handled in this pipeline are:
- Data type handling
- Handle duplicacy for categorical fields
- Domain validation for numeric and categorical fields
- Data extraction from large strings
- Data cleaning
- Data manipulation and preparation
- Generate statistically preprocessed collections
- Extract data with dependencies for `GraphQL`-like implementation

This repository exemplifies construction of dynamic pipelines following MongoDB Aggregation Framework for data extraction and conditional update queries using python scripts.
