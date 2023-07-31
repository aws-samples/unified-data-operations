from typing import List, Dict
from pyspark import SparkConf
from driver.task_executor import DataSet


def enrich_spark_conf(conf: SparkConf) -> SparkConf:
    conf.set("spark.sql.warehouse.dir", "some warehouse location")
    return conf


def add_post_processors() -> List[callable]:
    # def my_custom_post_processor(data_set: DataSet) -> DataSet:
    #     return data_set.df
    #
    # return [my_custom_post_processor]
    return list()


def add_pre_processors() -> List[callable]:
    return list()


def add_transformers() -> List[Dict[str, callable]]:
    return dict()
