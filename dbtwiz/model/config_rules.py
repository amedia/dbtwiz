"""Generic assertions on a model's own yml `config` block.

dbtwiz has no built-in notion of what any rule means. A project declares conditions and
assertions over dotted paths into a model's `config` block in its own pyproject.toml, so
domain naming - classification flags, retention categories, lifecycle variables - stays
entirely in the consuming project:

    [[tool.dbtwiz.project.model_config_rules]]
    when    = { "meta.<a flag the project defines>" = true }
    max     = { "partition_expiration_days" = "{{ var('<a project variable>') }}" }
    message = "<why this rule exists>"

A rule applies when every `when` condition matches (no `when` means it always applies),
and then each assertion is checked:

- `require`: the paths listed must be set
- `forbid`: path -> values the path must not have
- `max`/`min`: path -> numeric bound, checked only when the path is set

Numeric values and bounds are resolved through `{{ var('...') }}` references, so a rule
compares the number of days a model actually gets rather than the spelling of a variable
reference. Rules are checked against the model's own yml only - config inherited from
dbt_project.yml is not visible here.
"""

from typing import Any, Dict, List

from ..utils.jinja import resolve_number
from ..utils.logger import fatal

# Keys a single rule may declare, and those of them that assert something. A rule with no
# assertion key would silently do nothing, so the config layer rejects it.
RULE_KEYS = ("when", "require", "forbid", "max", "min", "message")
ASSERTION_KEYS = ("require", "forbid", "max", "min")


def config_value(config: Dict[str, Any], path: str) -> Any:
    """Look up a dotted path in a model's config block, e.g. `meta.owner`.

    Args:
        config: The model's `config` block
        path: Dotted path to look up

    Returns:
        The value at that path, or None if any step along the path is missing
    """
    value: Any = config
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def rule_applies(rule: Dict[str, Any], config: Dict[str, Any]) -> bool:
    """Check whether every `when` condition of a rule matches the model's config.

    Conditions compare by equality, which keeps them predictable for the flags and enums
    they are meant for (a project's own meta flags, or `materialized = "incremental"`). A
    rule without `when` always applies.
    """
    return all(
        config_value(config, path) == expected
        for path, expected in (rule.get("when") or {}).items()
    )


def rule_violations(
    rule: Dict[str, Any], config: Dict[str, Any], variables: Dict[str, Any]
) -> List[str]:
    """Check a single applicable rule against a model's config.

    Args:
        rule: One validated rule from `[[tool.dbtwiz.project.model_config_rules]]`
        config: The model's `config` block
        variables: Project variables, as merged from dbt_project.yml and vars.yml

    Returns:
        One message per violation, empty when the rule is satisfied
    """
    violations = [
        *_require_violations(rule, config),
        *_forbid_violations(rule, config, variables),
        *_bound_violations(rule, config, variables, "max"),
        *_bound_violations(rule, config, variables, "min"),
    ]

    # A rule's own message explains the intent behind it, which the mechanical
    # description of a violation cannot.
    message = rule.get("message")
    if message and violations:
        return [f"{violation}\n  -> {message}" for violation in violations]
    return violations


def evaluate_rules(
    config: Dict[str, Any], rules: List[Dict[str, Any]], variables: Dict[str, Any]
) -> List[str]:
    """Check a model's config against every applicable rule.

    Args:
        config: The model's `config` block
        rules: Validated rules from `[[tool.dbtwiz.project.model_config_rules]]`
        variables: Project variables, as merged from dbt_project.yml and vars.yml

    Returns:
        One message per violation across all rules, empty when all rules are satisfied
    """
    violations: List[str] = []
    for rule in rules:
        if rule_applies(rule, config):
            violations.extend(rule_violations(rule, config, variables))
    return violations


def _require_violations(rule: Dict[str, Any], config: Dict[str, Any]) -> List[str]:
    """Check the `require` assertion: every listed path must be set."""
    return [
        f"'{path}' is not set"
        for path in rule.get("require") or []
        if config_value(config, path) is None
    ]


def _forbid_violations(
    rule: Dict[str, Any], config: Dict[str, Any], variables: Dict[str, Any]
) -> List[str]:
    """Check the `forbid` assertion: a path that is set must not have a listed value."""
    violations: List[str] = []
    for path, forbidden in (rule.get("forbid") or {}).items():
        actual = config_value(config, path)
        if actual is None:
            continue
        if any(_values_match(actual, value, variables) for value in forbidden):
            violations.append(
                f"'{path}' is {_render(actual, variables)}, which is not allowed"
            )
    return violations


def _bound_violations(
    rule: Dict[str, Any], config: Dict[str, Any], variables: Dict[str, Any], key: str
) -> List[str]:
    """Check a `max` or `min` assertion, resolving both the bound and the model's value.

    A path that is not set is left to `require`; there is no bound to compare against.
    """
    bound_name = "maximum" if key == "max" else "minimum"
    violations: List[str] = []

    for path, raw_bound in (rule.get(key) or {}).items():
        bound = resolve_number(raw_bound, variables)
        if bound.error:
            # A bound comes from pyproject.toml, so an unresolvable one is a config bug
            # rather than a model problem: fail loudly instead of skipping the check.
            fatal(
                f"The '{key}' bound for '{path}' in "
                f"[[tool.dbtwiz.project.model_config_rules]] cannot be resolved: "
                f"{bound.error}"
            )

        actual = config_value(config, path)
        if actual is None:
            continue

        value = resolve_number(actual, variables)
        if value.error:
            # Never pass silently: an unresolvable value means the rule went unchecked.
            violations.append(
                f"'{path}' cannot be checked against the {bound_name} of "
                f"{bound.source} - {value.error}"
            )
        elif value.value > bound.value if key == "max" else value.value < bound.value:
            violations.append(
                f"'{path}' is {value.source}, "
                f"{'above' if key == 'max' else 'below'} the "
                f"{bound_name} of {bound.source}"
            )

    return violations


def _values_match(actual: Any, expected: Any, variables: Dict[str, Any]) -> bool:
    """Compare two config values, numerically when both sides resolve to numbers.

    Equality covers flags and enums (`materialized = "ephemeral"`), while numeric
    comparison lets a rule forbid a retention length without having to match the exact
    spelling of the variable reference a model happens to use for it.
    """
    if actual == expected:
        return True
    actual_number = resolve_number(actual, variables)
    expected_number = resolve_number(expected, variables)
    if actual_number.value is None or expected_number.value is None:
        return False
    return actual_number.value == expected_number.value


def _render(value: Any, variables: Dict[str, Any]) -> str:
    """Render a config value for a message, naming the variable behind it when there is one."""
    resolved = resolve_number(value, variables)
    return resolved.source if resolved.error is None else f"'{value}'"
