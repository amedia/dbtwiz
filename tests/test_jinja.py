"""Tests for parsing and resolving dbt jinja variable references."""

import pytest

from dbtwiz.utils.jinja import parse_var_ref, resolve_number

VARIABLES = {
    "short-retention": 30,
    "standard-retention": 550,
    "capped-retention": 1096,
    "unlimited-retention": 10000,
}


class TestParseVarRef:
    """Test extraction of a variable reference from a config value."""

    @pytest.mark.parametrize(
        "source",
        [
            "{{ var('capped-retention') }}",
            "{{ var('capped-retention')}}",
            "{{var('capped-retention')}}",
            '{{ var("capped-retention") }}',
            "{{ var('capped-retention')     }}",
            "  {{ var('capped-retention') }}  ",
            "{{\n  var('capped-retention')\n}}",
        ],
    )
    def test_accepts_any_spelling_of_a_plain_reference(self, source):
        """Quoting, spacing and surrounding whitespace do not change the reference."""
        assert parse_var_ref(source) == ("capped-retention", None)

    def test_reads_an_inline_default(self):
        """A default argument is returned alongside the name, keeping its type."""
        assert parse_var_ref("{{ var('missing-var', 90) }}") == ("missing-var", 90)

    @pytest.mark.parametrize(
        "source",
        [
            "1000",
            "{{ var('x') * 2 }}",
            "{{ var('x') or 90 }}",
            "{{ var('a') }}{{ var('b') }}",
            "prefix{{ var('x') }}",
            "{{ env_var('FOO') }}",
            "{{ var(some_name) }}",
            "{{ var('unterminated' }}",
        ],
    )
    def test_refuses_anything_but_a_plain_reference(self, source):
        """Literals and compound expressions are not mistaken for a reference."""
        assert parse_var_ref(source) is None


class TestResolveNumber:
    """Test resolving config values and rule bounds to numbers."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            (1000, 1000),
            (1000.5, 1000.5),
            ("1000", 1000),
            ("  1000  ", 1000),
            ("{{ var('capped-retention') }}", 1096),
            ("{{ var('capped-retention')     }}", 1096),
            ('{{ var("unlimited-retention") }}', 10000),
            ("{{ var('missing-var', 90) }}", 90),
        ],
    )
    def test_resolves_literals_and_references(self, value, expected):
        """Every accepted spelling of a number resolves to the same value."""
        assert resolve_number(value, VARIABLES).value == expected

    def test_names_the_variable_it_resolved(self):
        """The rendered source names the variable, so messages can explain themselves."""
        resolved = resolve_number("{{ var('unlimited-retention') }}", VARIABLES)
        assert resolved.source == "10000 (var 'unlimited-retention')"

    @pytest.mark.parametrize(
        "value,expected_error",
        [
            (None, "no value is set"),
            (True, "not a number"),
            ("soon", "neither a number nor a plain"),
            ("{{ var('undeclared') }}", "not declared in dbt_project.yml or vars.yml"),
            ("{{ var('capped-retention') * 2 }}", "neither a number nor a plain"),
        ],
    )
    def test_explains_what_it_could_not_resolve(self, value, expected_error):
        """An unresolvable value carries a reason instead of silently becoming a number."""
        resolved = resolve_number(value, VARIABLES)
        assert resolved.value is None
        assert expected_error in resolved.error

    def test_rejects_a_non_numeric_variable(self):
        """A variable that exists but holds text cannot stand in for a number."""
        resolved = resolve_number("{{ var('owner') }}", {"owner": "team-a"})
        assert resolved.value is None
        assert "not a number" in resolved.error
