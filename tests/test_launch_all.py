"""launch_all.plan: one launch per dataset job, latest partition, layers first."""

from wprdc_etl import definitions as d
from wprdc_etl.launch_all import LAYER_PRIORITY, plan


def test_plan_covers_every_dataset_job_and_no_maintenance() -> None:
    launches = plan(d.defs)
    names = [launch.job for launch in launches]
    assert len(names) == len(set(names))
    assert not any(name.startswith("maintenance__") for name in names)
    assert "allegheny_county__gis__basins__job" in names


def test_layers_first_with_priority() -> None:
    launches = plan(d.defs)
    layer = [x.tags.get("dagster/priority") == LAYER_PRIORITY for x in launches]
    assert sum(layer) == 16
    assert layer == sorted(layer, reverse=True)  # all layers before the rest


def test_partitioned_jobs_get_their_latest_partition() -> None:
    (basins,) = plan(d.defs, only="__gis__basins__")
    assert basins.partition and basins.tags["dagster/partition"] == basins.partition


def test_filters() -> None:
    pasda = plan(d.defs, source="pasda")
    assert {x.job.split("__")[2] for x in pasda} == {
        "address_points",
        "building_footprints",
        "parcels",
        "street_centerlines",
    }
    assert all("__wards__" in x.job for x in plan(d.defs, only="__wards__"))
    layers = plan(d.defs, layers=True)
    assert len(layers) == 16
    assert all(x.tags["dagster/priority"] == LAYER_PRIORITY for x in layers)
