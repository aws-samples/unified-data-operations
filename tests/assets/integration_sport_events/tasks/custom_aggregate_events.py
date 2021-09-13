from typing import List

from pyspark.sql.functions import col

from driver.common import get_data_set
from driver.core import DataSet


def execute(inp_dfs: List[DataSet]):
    events = get_data_set(inp_dfs, 'events').df.alias('events')
    teams = get_data_set(inp_dfs, 'teams').df.alias('teams')
    locations = get_data_set(inp_dfs, 'locations').df.alias('locations')

    events = events.join(locations, on=events.location_id == locations.id).select('events.*', col("locations.name").alias('location'))
    events = events.join(teams, on=events.home_team_id == teams.id).select('events.*', col('location'), col("teams.name").alias('home_team'))
    events = events.join(teams, on=events.away_team_id == teams.id).select('events.*', col('location'), col('home_team'), col("teams.name").alias('away_team'))

    events = events.drop(col('location_id'))
    events = events.drop(col('home_team_id'))
    events = events.drop(col('away_team_id'))

    output_ds = DataSet(model_id='calendar', df=events, input_id=None, model=None, product=None)
    return [output_ds]
