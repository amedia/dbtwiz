"""sqlfluff's CLI with dbt's package version check turned off.

`dbtwiz model format` runs sqlfluff as a subprocess, so the version check has to be
turned off inside that process: see `disable_sqlfluff_dbt_version_check`.
"""

from .run import disable_sqlfluff_dbt_version_check


def main() -> None:
    disable_sqlfluff_dbt_version_check()

    from sqlfluff.cli.commands import cli

    cli()


if __name__ == "__main__":
    main()
