from .auth import ensure_auth
from .dbt import dbt_invoke
from .logging import info


class Freshness():

    @classmethod
    def run(
            cls,
            target: str,
    ) -> None:
        info("Running source freshness tests")

        if target == "dev":
            ensure_auth()

        dbt_invoke(["source", "freshness"], target=target)
