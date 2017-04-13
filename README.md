# openoaklandbudget-data
A utility to process tabular CSV data into JSON fragments with indexing. 

# Rationale 
The purpose of this utility is to create a static index of raw financial tabular data, like that of City of Oakland budget, so that it maybe queried via an API. A more traditional approach might be to load the data into a SQL database or a document database like MongoDB. This utility is designed with the opinion that because the data is updated with a very low frequency, i.e. once a year, an online database engine like MySQL or MongoDB would introduce an unnecessary level of complexity and would negatively impact hosting costs and service availability.

Instead the data is pre-queried according to an index definition and the results are stored in flat files (JSON) using a simple tree structure. These flat files can be included in a variety of projects, but are primarily intended to be uploaded to a content delivery network, so they may be efficiently deliveries on-demand to a web or native mobile application.

# Workflow
The process is to:
1. Aquire raw file and place in data directory
2. Run process.py
3. Review the generated files, specifically header.json and the files in the taxonomy directory.
4. Grab the keys for various fields, and specify an index in a configuration file in the data/raw directory named dataset.config.json where "dataset" is the name of the raw CSV file.
5. Run process.py again to generate the index (depending on the size of the file, and complexity of the index definition, this can take a while) 
6. Run upload.py (TBD, doesn't exist yet) to upload the files to a CDN (Amazon S3)

# API Access
An API can be configured to access the data as Amazon S3 proxy requests using a backend web application or Amazon API Gateway. This is preferred over accessing the data directly as it allows for easy control of CORS, authentication/authorization, and rate limiting. Also using an API facade to access the data allows changes to how the data is accessed without distrupting downstream consumers. 
