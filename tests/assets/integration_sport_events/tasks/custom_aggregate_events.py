from typing import List

from pyspark.sql.functions import col

from driver.common import find_dataset_by_id
from driver.core import DataSet


def execute(inp_dfs: List[DataSet]):
    events = find_dataset_by_id(inp_dfs, 'events').df.alias('events')
    teams = find_dataset_by_id(inp_dfs, 'teams').df.alias('teams')
    locations = find_dataset_by_id(inp_dfs, 'locations').df.alias('locations')

    events = events.join(locations, on=events.location_id == locations.id).select('events.*', col("locations.name").alias('location'))
    events = events.join(teams, on=events.home_team_id == teams.id).select('events.*', col('location'), col("teams.name").alias('home_team'))
    events = events.join(teams, on=events.away_team_id == teams.id).select('events.*', col('location'), col('home_team'), col("teams.name").alias('away_team'))

    events = events.drop(col('location_id'))
    events = events.drop(col('home_team_id'))
    events = events.drop(col('away_team_id'))

    output_ds = DataSet(id='calendar', df=events)
    return [output_ds]
