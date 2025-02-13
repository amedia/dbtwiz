from dbtwiz.auth import ensure_auth
from dbtwiz.dbt import dbt_invoke
from dbtwiz.logging import info


def freshness(target: str) -> None:
    info("Running source freshness tests")

    if target == "dev":
        ensure_auth()

    dbt_invoke(["source", "freshness"], target=target)
