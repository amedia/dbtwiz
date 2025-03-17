from typing import Dict, List

from dbtwiz.bigquery import BigQueryClient
from dbtwiz.interact import multiselect_from_list
from dbtwiz.logging import info
from dbtwiz.manifest import Manifest


# Extract partition expiration variables from the manifest
def extract_partition_vars(manifest: dict) -> dict:
    """Extract partition expiration variables from the manifest."""
    return manifest.get("metadata", {}).get("vars", {})


# Identify models with partition expiration defined
def identify_models_with_partition_expiration(manifest: dict) -> list:
    """Identify models with partition_expiration_days defined in the manifest."""
    models = []
    for _, node in manifest["nodes"].items():
        if node["resource_type"] == "model" and "config" in node and "partition_expiration_days" in node["config"]:
            models.append({
                "model_name": node["name"],
                "table_id": f"{node['database']}.{node['schema']}.{node['alias']}",
                "defined_expiration": node["config"]["partition_expiration_days"]
            })
    return models


# Convert defined expiration to actual values using partition_vars
def resolve_partition_expiration(models: list, partition_vars: dict) -> list:
    """Resolve partition expiration values using variables from the manifest."""
    for model in models:
        if isinstance(model["defined_expiration"], str) and model["defined_expiration"].startswith("{{ var("):
            var_name = model["defined_expiration"].split("'")[1]
            model["defined_expiration"] = partition_vars.get(var_name, 0)
    return models


# Compare defined expiration with BigQuery expiration
def find_mismatched_models(models: list, client: BigQueryClient) -> List[Dict[str, str]]:
    """Find models where the defined expiration does not match the BigQuery expiration."""
    mismatched_models = []
    for model in models:
        current_expiration = client.get_bigquery_partition_expiration(model["table_id"])
        if current_expiration != model["defined_expiration"]:
            table_id = model["table_id"]
            defined = model["defined_expiration"]
            current = current_expiration
            if current_expiration == -1:
                difference = model["defined_expiration"]
            else:
                difference = model["defined_expiration"] - current_expiration
            mismatched_models.append({
                # Attributes used by questionary
                "name": f"{table_id:<95} {current:>5} → {defined:>5} ({difference:>+1})",
                "value": table_id,
                "description": f"Current expiration is {current} days while defined expiration is {defined} days",
                # Additional attribute used when updating selected tables
                "defined_expiration": defined
            })

    return mismatched_models


def update_partition_expirations(
        model_names: List[str] = None,
):
    """Main function to compare and update partition expiration in BigQuery."""
    # Update and read the prod manifest
    Manifest.update_manifests("prod")
    prod_manifest = Manifest.get_manifest(Manifest.PROD_MANIFEST_PATH)

    info("Identifying mismatched partition expirations...")

    # Extract partition expiration variables from the prod manifest
    partition_vars = extract_partition_vars(prod_manifest)

    # Identify models with partition expiration
    models = identify_models_with_partition_expiration(prod_manifest)
    models = resolve_partition_expiration(models, partition_vars)

    # Filter models if model_names provided
    if model_names:
        models = [item for item in models if item['model_name'] in model_names]

    # Find mismatched models
    client = BigQueryClient()
    mismatched_models = find_mismatched_models(models, client)

    # Present mismatched models to the user
    if mismatched_models:
        # Prompt user to select tables to update
        selected_tables = multiselect_from_list(
            "Select mismatched tables to update",
            items=sorted(mismatched_models, key=lambda x: x["name"]),
            allow_none=True,
        )

        # Update selected tables
        for table_id in selected_tables:
            model = next(model for model in mismatched_models if model.get("value") == table_id)
            client.update_bigquery_partition_expiration(table_id, model["defined_expiration"])
    else:
        print("No mismatched models found.")
