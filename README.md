# `caa-streamworx`

caa-streamworx is a code repository for the backend data pipeline development of a CRA data science project aimed at investigating the security value of siteminder logs with a view towards automated intrusion detection. This work is being undertaken as part of a BCIP initiative in collaboration with streamworx.ai, and development work here is being carried out by AISS and CAA teams at the CRA.

The code in this repository is written in python and meant to be deployed on a databricks service running in a CRA azure subscription. Thus, it is largely a spark-based project. Since the deployment takes place in Azure, some dev ops are staged in the appropriate azure services, and some additional development takes place in notebooks which are not version-controlled here.

# Installation/Deployment
[Deployment of a new release on databricks using Azure Dev Ops](https://github.com/CRA-CAA/caa-streamworx/files/6894297/library.Release.pdf)

# Project Structure
This project consists of three broad categories of assets for constructing pipelines and machine learning models:

- Scripts for performing repeatable tasks, like ingesting data from a fixed source in a specific format
- Transformers, implemented as python classes extending the appropriate notions in either scikitlearn or spark
- Development related items like unit tests and test data.

Spark related scripts and transformers are located in `/src/caaswx/spark/`. Testing modules are located in `/tests/`. Testing data is located in `/data/`.

# Sample Usage
In a databricks notebook:

```python
import caaswx

df = table("raw_logs")
feature_generator = caaswx.spark.transformers.UserFeatureGenerator(window_step = 900, window_length = 600)

feature_generator.transform(df).take(50)
```

# License
[MIT](https://choosealicense.com/licenses/mit/)
