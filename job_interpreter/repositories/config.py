import yaml

from pathlib import Path
from pyspark import SparkFiles
from pyspark.context import SparkContext


class ConfigRepository:
    context: SparkContext

    def __init__(self, context: SparkContext):
        self.context = context

    def get(self, path: str):
        try:
            model_str = Path(path).read_text()
        except FileNotFoundError:
            model_file = self.context.textFile(SparkFiles.get(path))
            model_str = '\n'.join(model_file.collect())

        return yaml.safe_load(model_str)