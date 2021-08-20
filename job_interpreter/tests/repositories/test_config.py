import os

from ...repositories import ConfigRepository


def test_get(spark_session):
    spark_session.sparkContext.addFile(os.path.abspath('./job_interpreter/tests/repositories/foo.yml'))

    model = ConfigRepository(spark_session.sparkContext).get('./foo.yml')

    assert model.get('foo') == 'bar'
