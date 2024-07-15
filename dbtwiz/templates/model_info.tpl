[b]Model:[/]
[name]{{ model["name"] }}[/]

[b]Path:[/]
[path]{{ model["folder"] }}[/]

[b]Tags:[/]
{% for tag in model["tags"] -%}
[tags]{{ tag }}[/]{% if not loop.last %}, {% endif %}
{%- endfor %}

[b]Group:[/]
[group]{{ model["group"] }}[/]

[b]Materialized:[/]
[materialized]{{ model["materialized"] }}[/]

{% if "owner" in model["meta"] -%}
[b]Owner:[/]
[owner]{{ model["meta"]["owner"] }}[/]
{% endif %}

{% if "access-policy" in model["meta"] -%}
[b]Access policy:[/]
[policy]{{ model["meta"]["access-policy"] }}[/]
{% endif %}

{% if "lifecycle-policy" in model["meta"] -%}
[b]Lifecycle policy:[/]
[policy]{{ model["meta"]["lifecycle-policy"] }}[/]
{% endif %}

{% if model["parent_models"] | length > 0 -%}
[b]Parent models:[/]
{% for parent in model["parent_models"] -%}
{{ model_style(parent) }}{{ parent }}[/]
{% endfor %}
{% endif %}

{% if model["child_models"] | length > 0 -%}
[b]Child models:[/]
{% for child in model["child_models"] -%}
{{ model_style(child) }}{{ child }}[/]
{% endfor %}
{% endif %}

[b]Description:[/]
{% if model["deprecated"] %}[deprecated]{% else %}[description]{% endif -%}
{{ model["description"].strip() }}[/]
