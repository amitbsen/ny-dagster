"""Tests for landmarks and SBS by-BIN aggregators."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from transforms.logic.transforms.aggregate_landmarks_by_bin import (
    aggregate_landmarks_by_bin,
)
from transforms.logic.transforms.aggregate_sbs_by_bin import aggregate_sbs_by_bin


def test_aggregate_landmarks_collapses_bins_and_filters_noise(
    con: DuckDBPyConnection,
) -> None:
    con.execute(
        """
        CREATE TABLE landmarks AS
        SELECT * FROM (VALUES
            ('B1', 'Soho', 'Gothic', 'Smith', 'Brick', 'Hotel', '1'),
            ('B1', 'Soho', 'Not determined', 'Jones', '0', 'Hotel', '1'),
            ('B2', '', 'Victorian', NULL, 'Stone', 'Museum', '0')
        ) AS t(bin, historic_district, style_primary, architect_builder,
               material_primary, use_original, landmark_original)
        """
    )
    result = aggregate_landmarks_by_bin(con).fetchall()
    rows = {r[0]: r for r in result}
    assert set(rows.keys()) == {"B1", "B2"}
    b1 = rows["B1"]
    # historic_districts agg
    assert b1[2] == "Soho"
    # styles agg drops "Not determined"
    assert b1[3] == "Gothic"
    # architects agg drops NULL and includes both names
    assert b1[4] is not None and "Smith" in b1[4] and "Jones" in b1[4]
    # materials agg drops "0"
    assert b1[5] == "Brick"
    # landmark_entry_count
    assert b1[8] == 2


def test_aggregate_sbs_collapses_bins(con: DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE sbs_certified_businesses AS
        SELECT * FROM (VALUES
            ('B1', 'MWBE', 'Acme LLC', 'Asian', 'Retail'),
            ('B1', 'WBE', 'Acme LLC', 'Asian', 'Retail'),
            ('B2', 'MBE', 'Beta Inc', '', 'Services')
        ) AS t(bin, certification, vendor_formal_name, ethnicity, naics_sector)
        """
    )
    rows = {r[0]: r for r in aggregate_sbs_by_bin(con).fetchall()}
    assert set(rows.keys()) == {"B1", "B2"}
    # B1 count = 2
    assert rows["B1"][2] == 2
    certs = rows["B1"][3]
    assert certs is not None and "MWBE" in certs and "WBE" in certs
