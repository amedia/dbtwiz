from pathlib import Path
import subprocess


from .logging import info, fatal


class SqlFix():

    QUERY_FOLDERS = [
        "models", "macros", "tests", "seeds", "analyses"
    ]

    @classmethod
    def run(cls) -> None:
        query_files = cls.staged_queries()
        if len(query_files) == 0:
            info("No staged SQL changes detected.")
            return

        info("Running sqlfmt on changed SQL files.")
        subprocess.run(["sqlfmt", "--line-length=100"] + query_files)

        info("Running sqlfluff fix on changes SQL files.")
        subprocess.run(["sqlfluff", "fix"] + query_files)


    @classmethod
    def staged_queries(cls):
        git_status = subprocess.run([
            "git", "status", "--short", "--untracked-files=no",
            "--no-ahead-behind", "--no-renames"
        ], capture_output=True)
        if git_status.returncode > 0:
            fatal(git_status.stderr.decode("utf-8"))

        query_files = list()
        for line in git_status.stdout.decode("utf-8").splitlines():
            staged, *_, filename = line.split(" ")
            if staged in ["A", "M"]:
                path = Path(filename)
                if path.parts[0] in cls.QUERY_FOLDERS and path.suffix == ".sql":
                    query_files.append(path)

        return query_files
