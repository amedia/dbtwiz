"""Tests for model config rules: variable resolution, rule evaluation and config shape."""

import pytest
import typer

from dbtwiz.config.project import ProjectConfig
from dbtwiz.model.config_rules import config_value, evaluate_rules
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


class TestConfigValue:
    """Test dotted path lookup into a model's config block."""

    CONFIG = {"materialized": "incremental", "meta": {"owner": "team-a"}}

    def test_reads_a_nested_path(self):
        """A dotted path addresses keys below config."""
        assert config_value(self.CONFIG, "meta.owner") == "team-a"

    def test_reads_a_top_level_path(self):
        """A path without dots addresses a config key directly."""
        assert config_value(self.CONFIG, "materialized") == "incremental"

    @pytest.mark.parametrize(
        "config,path",
        [
            (CONFIG, "meta.missing"),
            (CONFIG, "missing.owner"),
            (CONFIG, "materialized.owner"),
            ({"meta": None}, "meta.owner"),
            ({}, "meta.owner"),
        ],
    )
    def test_missing_and_empty_paths_read_as_unset(self, config, path):
        """A path that is absent, or whose parent is empty, is unset rather than an error."""
        assert config_value(config, path) is None


class TestEvaluateRules:
    """Test rule evaluation against a model's config block."""

    RULES = [
        {
            "require": ["meta.classification"],
            "message": "Every model must declare a classification",
        },
        {
            "when": {"meta.classification": "sensitive"},
            "max": {"partition_expiration_days": "{{ var('capped-retention') }}"},
            "message": "Sensitive data must expire no later than the cap",
        },
        {
            "when": {"meta.classification": "sensitive", "materialized": "incremental"},
            "require": ["partition_expiration_days"],
            "message": "Incremental sensitive data must declare an expiration",
        },
    ]

    def test_no_rules_is_a_no_op(self):
        """A project that declares no rules gets no violations."""
        assert evaluate_rules({"materialized": "view"}, [], VARIABLES) == []

    def test_compliant_model_passes(self):
        """A model satisfying every applicable rule reports nothing."""
        config = {
            "materialized": "incremental",
            "meta": {"classification": "sensitive"},
            "partition_expiration_days": "{{ var('standard-retention') }}",
        }
        assert evaluate_rules(config, self.RULES, VARIABLES) == []

    def test_reports_a_missing_required_path(self):
        """A required path that is not set is reported with the rule's message."""
        violations = evaluate_rules({"materialized": "view"}, self.RULES, VARIABLES)
        assert len(violations) == 1
        assert "'meta.classification' is not set" in violations[0]
        assert "Every model must declare a classification" in violations[0]

    def test_empty_meta_block_reads_as_missing(self):
        """A `meta:` key present but empty is a missing tag, not a crash."""
        config = {"materialized": "view", "meta": None}
        violations = evaluate_rules(config, self.RULES, VARIABLES)
        assert len(violations) == 1
        assert "'meta.classification' is not set" in violations[0]

    def test_empty_config_block_reads_as_missing(self):
        """A model whose config block is empty still gets its required paths checked."""
        violations = evaluate_rules({}, self.RULES, VARIABLES)
        assert len(violations) == 1
        assert "'meta.classification' is not set" in violations[0]

    @pytest.mark.parametrize(
        "expiration",
        [
            "{{ var('unlimited-retention') }}",
            "{{ var('unlimited-retention')}}",
            '{{ var("unlimited-retention") }}',
            10000,
            "10000",
        ],
    )
    def test_reports_a_value_above_the_bound_however_it_is_written(self, expiration):
        """The bound compares resolved numbers, not the spelling of a reference."""
        config = {
            "materialized": "incremental",
            "meta": {"classification": "sensitive"},
            "partition_expiration_days": expiration,
        }
        violations = evaluate_rules(config, self.RULES, VARIABLES)
        assert len(violations) == 1
        assert "above the maximum of 1096 (var 'capped-retention')" in violations[0]

    def test_a_rule_only_applies_when_its_conditions_match(self):
        """A model not matching `when` is left alone by that rule."""
        config = {
            "materialized": "incremental",
            "meta": {"classification": "public"},
            "partition_expiration_days": "{{ var('unlimited-retention') }}",
        }
        assert evaluate_rules(config, self.RULES, VARIABLES) == []

    def test_all_conditions_must_match(self):
        """A rule with two conditions is skipped when only one of them matches."""
        config = {"materialized": "view", "meta": {"classification": "sensitive"}}
        assert evaluate_rules(config, self.RULES, VARIABLES) == []

    def test_reports_a_value_it_cannot_resolve(self):
        """An unresolvable value is reported, so a rule never silently goes unchecked."""
        config = {
            "materialized": "incremental",
            "meta": {"classification": "sensitive"},
            "partition_expiration_days": "{{ var('capped-retention') * 2 }}",
        }
        violations = evaluate_rules(config, self.RULES, VARIABLES)
        assert len(violations) == 1
        assert "cannot be checked against the maximum" in violations[0]

    def test_forbid_matches_by_value(self):
        """A forbidden number is caught behind a variable reference."""
        rules = [{"forbid": {"partition_expiration_days": [10000]}}]
        config = {"partition_expiration_days": "{{ var('unlimited-retention') }}"}
        violations = evaluate_rules(config, rules, VARIABLES)
        assert len(violations) == 1
        assert "which is not allowed" in violations[0]

    def test_forbid_matches_plain_values(self):
        """Non-numeric values are compared as they are."""
        rules = [{"forbid": {"materialized": ["ephemeral"]}}]
        assert evaluate_rules({"materialized": "ephemeral"}, rules, VARIABLES)
        assert evaluate_rules({"materialized": "view"}, rules, VARIABLES) == []

    def test_min_bound(self):
        """A `min` bound reports values below it."""
        rules = [{"min": {"partition_expiration_days": "{{ var('short-retention') }}"}}]
        violations = evaluate_rules({"partition_expiration_days": 7}, rules, VARIABLES)
        assert len(violations) == 1
        assert "below the minimum of 30 (var 'short-retention')" in violations[0]

    def test_bound_is_not_checked_when_the_path_is_unset(self):
        """`max`/`min` compare a value; requiring one is `require`'s job."""
        rules = [{"max": {"partition_expiration_days": 1096}}]
        assert evaluate_rules({"materialized": "view"}, rules, VARIABLES) == []

    def test_an_unresolvable_bound_is_a_config_error(self):
        """A bound that cannot be resolved comes from pyproject.toml, so it exits."""
        rules = [{"max": {"partition_expiration_days": "{{ var('undeclared') }}"}}]
        with pytest.raises(typer.Exit):
            evaluate_rules({"partition_expiration_days": 30}, rules, VARIABLES)


class TestModelConfigRuleEntries:
    """Test validation of rule shapes read from pyproject.toml."""

    @staticmethod
    def _config_with(rules):
        """Build a config the way _parse_config does: assigned, not validated."""
        config = ProjectConfig()
        config.model_config_rules = rules
        return config

    def test_no_rules_is_valid(self):
        """A project without the setting gets an empty rule list."""
        assert self._config_with([]).model_config_rule_entries() == []

    def test_valid_rules_pass_through(self):
        """A well-formed rule is returned unchanged."""
        rules = [
            {
                "when": {"meta.classification": "sensitive"},
                "require": ["partition_expiration_days"],
                "forbid": {"materialized": ["ephemeral"]},
                "max": {"partition_expiration_days": 1096},
                "min": {"partition_expiration_days": 30},
                "message": "Sensitive data must expire",
            }
        ]
        assert self._config_with(rules).model_config_rule_entries() == rules

    @pytest.mark.parametrize(
        "rules",
        [
            "not-a-list",
            [42],
            [{"when_meta": {"a": 1}, "require": ["x"]}],
            [{"when": {"a": 1}}],
            [{"when": "meta.classification", "require": ["x"]}],
            [{"require": "meta.classification"}],
            [{"require": [1]}],
            [{"forbid": {"materialized": "ephemeral"}}],
            [{"max": {"partition_expiration_days": [1, 2]}}],
            [{"min": {"partition_expiration_days": {"days": 30}}}],
            [{"require": ["x"], "message": 42}],
        ],
    )
    def test_malformed_rules_are_rejected(self, rules):
        """Types are not enforced on assignment, so each rule shape is checked here."""
        with pytest.raises(typer.Exit):
            self._config_with(rules).model_config_rule_entries()
