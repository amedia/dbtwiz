import re
from typing import Dict, List, Tuple

from dbtwiz.auth import ensure_auth
from dbtwiz.logging import fatal, info, warn
from dbtwiz.manifest import Manifest


def build_reference_lookup(manifest: dict) -> Dict[str, Tuple[str, str]]:
    """Build lookup dictionary from manifest."""
    lookup_dict = {}

    # Process models
    for node in manifest.models().values():
        if not node.get("unique_id").startswith("model."):
            continue

        database = node.get("database", "").lower()
        schema = node.get("schema", "").lower()
        name = node.get("name", "")
        alias = node.get("alias", name)

        if database and schema and name:
            key = f"{database}.{schema}.{alias}"
            lookup_dict[key] = ("ref", name)

    # Process sources
    for source in manifest.sources().values():
        if not source.get("unique_id").startswith("source."):
            continue

        database = source.get("database", "").lower()
        schema = source.get("schema", "").lower()
        name = source.get("name", "")
        identifier = source.get("identifier", name).lower()
        source_name = source.get("source_name", "")

        if database and schema and name and source_name:
            key = f"{database}.{schema}.{identifier}"
            lookup_dict[key] = ("source", (source_name, name))

    return lookup_dict


def replace_table_references(
    sql_content: str, lookup_dict: dict
) -> Tuple[str, List[str]]:
    """Replace table references while handling all backtick cases."""
    pattern = r"(`?[^`\s]+`?)\.(`?[^`\s]+`?)\.(`?[^`\s]+`?)"
    unresolved_tables = []

    def get_replacement(match):
        # Extract and clean each component
        project = match.group(1).strip("`")
        dataset = match.group(2).strip("`")
        table = match.group(3).strip("`")

        lookup_key = f"{project.lower()}.{dataset.lower()}.{table.lower()}"
        reference = lookup_dict.get(lookup_key)

        if reference:
            ref_type, ref_value = reference
            if ref_type == "ref":
                return f'{{{{ ref("{ref_value}") }}}}'
            else:
                source_name, table_name = ref_value
                return f'{{{{ source("{source_name}", "{table_name}") }}}}'
        else:
            unresolved_tables.append(f"{project}.{dataset}.{table}")
            return match.group(0)  # Return original with backticks if any

    new_sql = []
    last_pos = 0

    # Process content sequentially
    for match in re.finditer(pattern, sql_content):
        # Add text before match
        new_sql.append(sql_content[last_pos : match.start()])
        # Add replacement (which handles backtick removal)
        new_sql.append(get_replacement(match))
        last_pos = match.end()

    # Add remaining content
    new_sql.append(sql_content[last_pos:])
    return "".join(new_sql), list(set(unresolved_tables))


def convert_sql_to_model(file_path: str):
    """Converts a sql file to a dbt model by replacing full table references with ref and source."""
    if not file_path:
        fatal("Unable to convert sql to model since no file was specified.")

    ensure_auth()

    Manifest.download_prod_manifest()
    manifest_data = Manifest(Manifest.PROD_MANIFEST_PATH)

    lookup_dict = build_reference_lookup(manifest_data)

    with open(file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    new_sql, unresolved = replace_table_references(sql_content, lookup_dict)

    if new_sql != sql_content:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(new_sql)
        info(f"Updated references in {file_path}")
    else:
        info(f"No references replaced in {file_path}")

    if unresolved:
        warn("Unresolved tables:\n  - " + "\n  - ".join(unresolved))
