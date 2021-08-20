import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from job_interpreter import TaskInterpreter

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TASK_ID'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

task_interpreter = TaskInterpreter(glueContext)

task_interpreter.interpret(args['TASK_ID'])

job.commit()
