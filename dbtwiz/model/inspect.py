from dbtwiz.logging import error
from dbtwiz.manifest import Manifest


def inspect_model(name: str) -> None:
    """Function for inspecting a model."""
    models = Manifest.models_cached()
    if name not in models.keys():
        name = Manifest.choose_models(name, multi=False)

    if name is None:
        error("No model selected.")
        return

    manifest = Manifest()
    manifest.model_by_name(name)
    # model = manifest.model_by_name(name)
    # print(Manifest().model_info_template().render(model=model))

    ancestors = sorted(
        [
            m[0].split(".")[-1]
            for m in manifest.model_dependencies_upstream(f"model.dbt_core.{name}")
        ],
        key=manifest.model_ordering,
    )

    descendants = sorted(
        [
            m[0].split(".")[-1]
            for m in manifest.model_dependencies_downstream(f"model.dbt_core.{name}")
        ],
        key=manifest.model_ordering,
    )

    if len(ancestors) > 0:
        print("Ancestors:")
        for ancestor in ancestors:
            print(f"- {ancestor}")
        print()

    if len(descendants) > 0:
        print("Descendants:")
        for descendant in descendants:
            print(f"- {descendant}")
        print()
