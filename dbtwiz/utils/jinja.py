"""Helpers for reading values out of dbt jinja expressions.

Model configs often express a numeric setting as a dbt variable reference rather than a
literal, e.g. `partition_expiration_days: "{{ var('customer-data-expiration') }}"`. To
compare such a setting against anything, the reference has to be resolved to the number
behind it.

Expressions are *parsed*, never rendered. `Environment.parse` builds an AST and executes
nothing, so it is safe on arbitrary model yml, needs no dbt context, and reliably tells us
whether an expression is a plain variable reference or something we should not try to
interpret. Rendering would require the full dbt context (`target`, `env_var`, macros) and
would execute template code, neither of which belongs in a validation step.
"""

from typing import Any, Dict, NamedTuple, Optional, Tuple

from jinja2 import Environment, nodes

_ENV = Environment()


class ResolvedNumber(NamedTuple):
    """Outcome of resolving a config value or rule bound to a number.

    Attributes:
        value: The resolved number, or None when it could not be resolved
        source: Human-readable rendering of the value, naming the variable it came from
        error: Why resolution failed, or None on success
    """

    value: Optional[float]
    source: str
    error: Optional[str]


def parse_var_ref(source: str) -> Optional[Tuple[str, Any]]:
    """Parse a lone `{{ var('name') }}` expression into its variable name and default.

    Args:
        source: The raw config value to parse

    Returns:
        Tuple of (variable name, inline default or None), or None if the expression is
        not a single plain `var()` reference - a literal, a compound expression such as
        `{{ var('x') * 2 }}`, a call to another function, or invalid jinja. Callers can
        therefore tell "not a variable reference" apart from "a reference I can resolve".
    """
    try:
        ast = _ENV.parse(source)
    except Exception:
        return None

    if len(ast.body) != 1 or not isinstance(ast.body[0], nodes.Output):
        return None

    # Surrounding whitespace parses to literal template data; ignore it, but keep any
    # non-blank literal so `prefix{{ var('x') }}` is not mistaken for a plain reference.
    parts = [
        node
        for node in ast.body[0].nodes
        if not (isinstance(node, nodes.TemplateData) and not node.data.strip())
    ]
    if len(parts) != 1 or not isinstance(parts[0], nodes.Call):
        return None

    call = parts[0]
    if not (isinstance(call.node, nodes.Name) and call.node.name == "var"):
        return None
    if not call.args or not isinstance(call.args[0], nodes.Const):
        return None

    default = (
        call.args[1].value
        if len(call.args) > 1 and isinstance(call.args[1], nodes.Const)
        else None
    )
    return call.args[0].value, default


def resolve_number(value: Any, variables: Dict[str, Any]) -> ResolvedNumber:
    """Resolve a value to a number, following a `var()` reference when it is one.

    Accepts a number (`1000`), a numeric string (`"1000"`) or a variable reference
    (`"{{ var('customer-data-expiration') }}"`, in any spacing or quoting), so the same
    coercion applies to a value read from a model's yml and to a bound read from
    pyproject.toml. A variable's own value is resolved the same way, and an inline
    default (`{{ var('x', 90) }}`) is used when the variable is not declared.

    Args:
        value: The raw value to resolve
        variables: Project variables, as merged from dbt_project.yml and vars.yml

    Returns:
        ResolvedNumber with either a value or an error explaining what could not be resolved
    """
    if value is None:
        return ResolvedNumber(None, "unset", "no value is set")

    # bool is a subclass of int, but a flag is not a quantity.
    if isinstance(value, bool):
        return ResolvedNumber(None, str(value), f"'{value}' is not a number")

    if isinstance(value, (int, float)):
        return ResolvedNumber(value, _format(value), None)

    if not isinstance(value, str):
        return ResolvedNumber(None, str(value), f"'{value}' is not a number")

    number = _to_number(value)
    if number is not None:
        return ResolvedNumber(number, _format(number), None)

    parsed = parse_var_ref(value)
    if parsed is None:
        return ResolvedNumber(
            None,
            value,
            f"'{value}' is neither a number nor a plain {{{{ var('...') }}}} reference",
        )

    name, default = parsed
    if name in variables:
        raw = variables[name]
    elif default is not None:
        raw = default
    else:
        return ResolvedNumber(
            None,
            value,
            f"var '{name}' is not declared in dbt_project.yml or vars.yml",
        )

    number = _to_number(raw) if not isinstance(raw, (int, float)) else raw
    if number is None or isinstance(raw, bool):
        return ResolvedNumber(
            None, value, f"var '{name}' is not a number (got '{raw}')"
        )

    return ResolvedNumber(number, f"{_format(number)} (var '{name}')", None)


def _to_number(value: Any) -> Optional[float]:
    """Convert a string to int or float, returning None when it is not numeric."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        pass
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _format(number: float) -> str:
    """Render a number without a trailing '.0' on whole floats."""
    if isinstance(number, float) and number.is_integer():
        return str(int(number))
    return str(number)
