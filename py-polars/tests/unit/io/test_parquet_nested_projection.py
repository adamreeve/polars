"""Nested projection pushdown in the parquet reader.

A scan whose output struct drops fields must only decode the parquet leaves it
keeps. Results must match a full read followed by a select.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import polars as pl
from polars.testing import assert_frame_equal

if TYPE_CHECKING:
    from pathlib import Path

    from polars._typing import EngineType

pytestmark = pytest.mark.write_disk()

IGNORE_EXTRA = pl.ScanCastOptions(extra_struct_fields="ignore")


@pytest.fixture
def nested_file(tmp_path: Path) -> Path:
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "spec": [
                {"a": 1, "b": "x", "c": [1, 2], "d": {"e": 1.0, "f": "p"}},
                None,
                {"a": 3, "b": None, "c": [], "d": {"e": None, "f": "q"}},
                {"a": 4, "b": "w", "c": [7], "d": None},
            ],
            "items": [
                [{"name": "n1", "image": "i1", "size": 10}],
                [],
                None,
                [
                    {"name": "n4", "image": None, "size": 40},
                    {"name": "n5", "image": "i5", "size": 50},
                ],
            ],
            "tail": ["t1", "t2", "t3", "t4"],
        }
    )
    path = tmp_path / "nested.parquet"
    df.write_parquet(path, row_group_size=2)
    return path


def _schema_with(path: Path, **overrides: pl.DataType) -> dict[str, pl.DataType]:
    schema = dict(pl.read_parquet_schema(path))
    schema.update(overrides)
    return schema


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
def test_struct_subset_matches_full_read(nested_file: Path, engine: EngineType) -> None:
    pruned = _schema_with(
        nested_file, spec=pl.Struct({"a": pl.Int64, "d": pl.Struct({"f": pl.String})})
    )
    out = (
        pl.scan_parquet(nested_file, schema=pruned, cast_options=IGNORE_EXTRA)
        .select("id", "spec")
        .collect(engine=engine)
    )
    # Oracle: pyarrow's own leaf projection of the same file.
    pq = pytest.importorskip("pyarrow.parquet")
    expected = pl.from_arrow(
        pq.ParquetFile(nested_file).read(columns=["id", "spec.a", "spec.d.f"])
    )
    assert isinstance(expected, pl.DataFrame)
    assert out.schema["spec"] == pruned["spec"]
    assert_frame_equal(out, expected)
    assert out["spec"].null_count() == 1
    assert out["spec"].struct.field("d").null_count() == 2


def test_list_of_struct_subset(nested_file: Path) -> None:
    pruned = _schema_with(nested_file, items=pl.List(pl.Struct({"image": pl.String})))
    out = (
        pl.scan_parquet(nested_file, schema=pruned, cast_options=IGNORE_EXTRA)
        .select(
            "id",
            pl.col("items")
            .list.eval(pl.element().struct.field("image"))
            .alias("images"),
        )
        .collect()
    )
    expected = pl.read_parquet(nested_file).select(
        "id",
        pl.col("items").list.eval(pl.element().struct.field("image")).alias("images"),
    )
    assert_frame_equal(out, expected)


def test_struct_subset_with_filter_and_slice(nested_file: Path) -> None:
    pruned = _schema_with(nested_file, spec=pl.Struct({"b": pl.String}))
    lf = pl.scan_parquet(nested_file, schema=pruned, cast_options=IGNORE_EXTRA)

    out = (
        lf.filter(pl.col("id") > 1)
        .select("id", pl.col("spec").struct.field("b"))
        .collect()
    )
    expected = pl.DataFrame({"id": [2, 3, 4], "b": [None, None, "w"]})
    assert_frame_equal(out, expected)

    out = lf.select("id", "spec").head(2).collect()
    assert out.to_dicts() == [{"id": 1, "spec": {"b": "x"}}, {"id": 2, "spec": None}]

    # Filter on a kept struct field.
    out = lf.filter(pl.col("spec").struct.field("b") == "w").select("id").collect()
    assert out["id"].to_list() == [4]


def test_struct_subset_reorders_and_inserts_missing(nested_file: Path) -> None:
    pruned = _schema_with(
        nested_file, spec=pl.Struct({"b": pl.String, "new": pl.Int32, "a": pl.Int64})
    )
    out = (
        pl.scan_parquet(
            nested_file,
            schema=pruned,
            cast_options=pl.ScanCastOptions(
                extra_struct_fields="ignore", missing_struct_fields="insert"
            ),
        )
        .select("spec")
        .collect()
    )
    assert out.schema["spec"] == pruned["spec"]
    assert out.to_dicts() == [
        {"spec": {"b": "x", "new": None, "a": 1}},
        {"spec": None},
        {"spec": {"b": None, "new": None, "a": 3}},
        {"spec": {"b": "w", "new": None, "a": 4}},
    ]


def test_struct_only_missing_fields_keeps_validity(nested_file: Path) -> None:
    # Nothing of the file struct is kept: the reader must still produce the right
    # length and outer validity.
    pruned = _schema_with(nested_file, spec=pl.Struct({"new": pl.Int32}))
    out = (
        pl.scan_parquet(
            nested_file,
            schema=pruned,
            cast_options=pl.ScanCastOptions(
                extra_struct_fields="ignore", missing_struct_fields="insert"
            ),
        )
        .select("spec")
        .collect()
    )
    assert out.to_dicts() == [
        {"spec": {"new": None}},
        {"spec": None},
        {"spec": {"new": None}},
        {"spec": {"new": None}},
    ]


def test_struct_subset_with_cast(nested_file: Path) -> None:
    # Int64 -> Float64 is a cast the policy allows; it must run after pruning.
    pruned = _schema_with(nested_file, spec=pl.Struct({"a": pl.Float64}))
    out = (
        pl.scan_parquet(
            nested_file,
            schema=pruned,
            cast_options=pl.ScanCastOptions(
                extra_struct_fields="ignore", integer_cast="allow-float"
            ),
        )
        .select("spec")
        .collect()
    )
    assert out.schema["spec"] == pl.Struct({"a": pl.Float64})
    assert out["spec"].struct.field("a").to_list() == [1.0, None, 3.0, 4.0]

    # Int64 -> Int32 is not an upcast: must raise, same as without pruning.
    pruned = _schema_with(nested_file, spec=pl.Struct({"a": pl.Int32}))
    with pytest.raises(pl.exceptions.SchemaError):
        pl.scan_parquet(
            nested_file,
            schema=pruned,
            cast_options=pl.ScanCastOptions(
                extra_struct_fields="ignore", integer_cast="upcast"
            ),
        ).select("spec").collect()


def test_struct_extra_fields_raise_still_raises(nested_file: Path) -> None:
    pruned = _schema_with(nested_file, spec=pl.Struct({"a": pl.Int64}))
    with pytest.raises(pl.exceptions.SchemaError, match="extra struct field"):
        pl.scan_parquet(nested_file, schema=pruned).select("spec").collect()
