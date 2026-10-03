"""ckan.bool_format: how a replace publishes boolean columns.

Production's DataPusher+ re-creates every table and can't keep a bool column,
so booleans are converted before publishing — to "True"/"False" text by
default, or 1/0 for older datasets that use that.
"""

import dagster as dg
import pandas as pd
import pytest

from wprdc_etl.strategies.load import publishable_booleans


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "inactive": [True, False],
            "maybe": pd.array([True, None], dtype="boolean"),
            "name": ["a", "b"],
        }
    )


def test_text_is_the_default_form() -> None:
    out = publishable_booleans(_frame(), "text")
    assert list(out["inactive"]) == ["True", "False"]
    assert out["maybe"][0] == "True" and pd.isna(out["maybe"][1])
    assert str(out["inactive"].dtype) == "string"


def test_int_for_older_datasets() -> None:
    out = publishable_booleans(_frame(), "int")
    assert list(out["inactive"]) == [1, 0]
    assert out["maybe"][0] == 1 and pd.isna(out["maybe"][1])
    assert str(out["inactive"].dtype) == "Int64"


def test_other_columns_and_the_input_are_untouched() -> None:
    frame = _frame()
    out = publishable_booleans(frame, "text")
    assert list(out["name"]) == ["a", "b"]
    assert frame["inactive"].dtype == bool  # converted a copy, not the input


def test_an_unknown_format_is_a_permanent_failure() -> None:
    with pytest.raises(dg.Failure) as exc:
        publishable_booleans(_frame(), "yes/no")
    assert exc.value.allow_retries is False
