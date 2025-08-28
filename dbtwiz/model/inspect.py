from ..core.project import Project
from ..dbt.manifest import Manifest
from ..utils.logger import error


def inspect_model(name: str) -> None:
    """Function for inspecting a model."""
    Manifest.models_cached()
    if not Manifest.can_select_directly(name):
        Manifest().update_models_info()
        name = Manifest.choose_models(name, multi=False)

    if name is None:
        error("No model selected.")
        return

    manifest = Manifest()
    manifest.model_by_name(name)

    ancestors = sorted(
        [
            m[0].split(".")[-1]
            for m in manifest.model_dependencies_upstream(
                f"model.{Project().name()}.{name}"
            )
        ],
        key=manifest.model_ordering,
    )

    descendants = sorted(
        [
            m[0].split(".")[-1]
            for m in manifest.model_dependencies_downstream(
                f"model.{Project().name()}.{name}"
            )
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
