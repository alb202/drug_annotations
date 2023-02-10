<h1 align="center">Drug Annotations</h3>

<div align="center">

  [![Status](https://img.shields.io/badge/status-active-success.svg)]() 
  [![GitHub Issues](https://img.shields.io/github/issues/alb202/drug_annotations.svg)](https://github.com/alb202/drug_annotations/issues)
  [![GitHub Pull Requests](https://img.shields.io/github/issues-pr/alb202/drug_annotations.svg)](https://github.com/alb202/drug_annotations/pulls)
  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<p align="center"> A dagster ETL pipeline to collect various open-source datasets for a drug annotation knowledge graph. 
    <br> 
</p>

## 📝 Table of Contents
- [About](#about)
- [Getting Started](#getting_started)
- [To-Do](#todo)
- [Authors](#authors)

## About <a name = "about"></a>

For each data source, the pipeline creates a set of edges and/or nodes between a selection of data types. An edge dataframe consists of the following columns: 
``` 
from_type: string - the type of data of the source node
from_value: string - the value of the source node
to_type: string - the type of data of the target node
to_value: string - the value of the target node
label: string - the type of edge
source: string - the source of the dataset
parameters: dictionary - a collection of lists of additional parameters for each edge
```
A node dataframe consists of the following columns:
``` 
node_type: string - the type of data of the node
value: string - the value of the node
source: string - the source of the dataset
parameters: dictionary - a collection of lists of additional parameters for each node
```

The valid node types and sources are located in the [formatting_config.yaml](src/config/formatting_config.yaml) file. To add additional valid inputs, just add to this file. They will be validated during the data formatting step.

## Getting Started <a name = "getting_started"></a>
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.


### Prerequisites
To run, you'll need [docker](https://docs.docker.com/get-docker/) installed on your machine


### Installing
Everything you need to run can be installed using the Dockerfile:


Clone the repository to a local folder and enter the root directory

```
git clone https://github.com/alb202/drug_annotations.git
cd drug_annotations
```

Build the docker container

```
docker build -t drug_annotations .
```

Launch the container. It is currently set to run on 'localhost', and the command will map the docker port (3000) to your port 8080. You'll also need to set a local folder as a volume to access the results of the pipeline.

```
docker run -dp 8080:3000 -v <local folder>:/opt/dagster/app/data drug_annotations
```
The local folder can be any empty, writable location. On Windows, try '%cd%/data'. On linux, try '$(pwd)/data'

Go to the address below and start the pipeline from the Dagit interface

```
localhost:8080
```

The output of the pipeline will appear in the folder mounted during the docker run command


## TODO <a name = "todo"></a>
Check back as more resources are added to the pipeline and the knowledge graph itself is completed


## Authors <a name = "authors"></a>
- [@alb202](https://github.com/alb202) - Idea & work

See also the list of [contributors](https://github.com/alb202/drug_annotations/contributors) who participated in this project.

