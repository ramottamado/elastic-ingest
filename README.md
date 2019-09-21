# Elastic Bulk Ingest

When use bulk ingest in elastic, the **_id** will be randomly assigned.
This script assign **_id** for every lines in your `json` data,
provided that the data generated with `orient=records` and `lines=true`
in pandas.
