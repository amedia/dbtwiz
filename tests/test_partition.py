"""Tests for resolving the partition expiration defined by a model."""

import pytest

from dbtwiz.admin.partition import resolve_partition_expiration

PARTITION_VARS = {
    "standard-retention": 550,
    "capped-retention": 1096,
}


def _resolve(defined):
    """Resolve a single defined expiration value."""
    return resolve_partition_expiration(
        [{"defined_expiration": defined}], PARTITION_VARS
    )[0]["defined_expiration"]


@pytest.mark.parametrize(
    "defined,expected",
    [
        ("{{ var('standard-retention') }}", 550),
        ("{{ var('standard-retention')}}", 550),
        ("{{var('standard-retention')}}", 550),
        # Double quotes used to raise IndexError in the previous string-splitting version.
        ('{{ var("capped-retention") }}', 1096),
        ("{{ var('capped-retention')     }}", 1096),
        # An inline default stands in for a variable the project does not declare.
        ("{{ var('absent-retention', 90) }}", 90),
        # A variable that is neither declared nor defaulted resolves to no expiration.
        ("{{ var('absent-retention') }}", 0),
    ],
)
def test_resolves_variable_references(defined, expected):
    """A reference resolves to its variable's value, whatever its spelling."""
    assert _resolve(defined) == expected


@pytest.mark.parametrize("defined", [30, "not-an-expression"])
def test_leaves_non_references_alone(defined):
    """A literal day count, or anything unparseable, is passed through untouched."""
    assert _resolve(defined) == defined
