import typer

from dbtwiz.bigquery import BigQueryClient
from dbtwiz.interact import multiselect_from_list
from dbtwiz.manifest import Manifest

# Initialize Typer CLI
app = typer.Typer()


# Extract partition expiration variables from the manifest
def extract_partition_vars(manifest: dict) -> dict:
    """Extract partition expiration variables from the manifest."""
    return manifest.get("metadata", {}).get("vars", {})

# Identify models with partition expiration defined
def identify_models_with_partition_expiration(manifest: dict) -> list:
    """Identify models with partition_expiration_days defined in the manifest."""
    models = []
    for node_id, node in manifest["nodes"].items():
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
def find_mismatched_models(models: list, client: BigQueryClient) -> list:
    """Find models where the defined expiration does not match the BigQuery expiration."""
    mismatched_models = []
    for model in models:
        # print(f"Checking partition expiration for table: {model['table_id']}...")
        current_expiration = client.get_bigquery_partition_expiration(model["table_id"])
        if current_expiration != model["defined_expiration"]:
            mismatched_models.append({
                "table_id": model["table_id"],
                "current_expiration": current_expiration,
                "defined_expiration": model["defined_expiration"],
                "difference": model["defined_expiration"] - current_expiration
            })
    return mismatched_models

# Format mismatched models for display
def format_mismatched_models(mismatched_models: list) -> list:
    """Format mismatched models for display in a clean tabular format."""
    formatted_choices = []
    for model in mismatched_models:
        table_id = model["table_id"]
        current = model["current_expiration"]
        defined = model["defined_expiration"]
        difference = model["difference"]
        # Align the numbers and arrows
        formatted_choices.append(
            {
                "name": f"{table_id:<95} {current:>5} → {defined:>5} ({difference:>+1})",
                "value": model["table_id"]
            }
        )
    return formatted_choices

@app.command()
def update_partition_expirations():
    """Main function to compare and update partition expiration in BigQuery."""
    print("Checking for mismatched partition expirations...")

    # Update and get the prod manifest
    Manifest.update_manifests("prod")
    prod_manifest = Manifest.get_manifest(Manifest.PROD_MANIFEST_PATH)

    # Extract partition expiration variables from the prod manifest
    partition_vars = extract_partition_vars(prod_manifest)

    # Identify models with partition expiration
    models = identify_models_with_partition_expiration(prod_manifest)
    models = resolve_partition_expiration(models, partition_vars)

    # Find mismatched models
    client = BigQueryClient()
    mismatched_models = find_mismatched_models(models, client)

    # Present mismatched models to the user
    if mismatched_models:
        # Format the choices for questionary.checkbox
        choices = format_mismatched_models(mismatched_models)
        # Add "Skip update" option
        choices.insert(0, {"name": "*** Skip update ***", "value": "skip"})

        # Prompt user to select tables to update
        selected_tables = multiselect_from_list(
            "Select mismatched tables to update",
            items=choices,
            allow_none=False,
        )

        # Update selected tables
        # if selected_tables and "skip" not in selected_tables:
        #     for table_id in selected_tables:
        #         model = next(model for model in mismatched_models if model["table_id"] == table_id)
        #         update_bigquery_partition_expiration(client, table_id, model["defined_expiration"])
        # else:
        #     print("Operation canceled.")
    else:
        print("No mismatched models found.")

# Run the CLI
if __name__ == "__main__":
    app()