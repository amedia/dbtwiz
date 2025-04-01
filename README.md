# dbtwiz
Python package with CLI helper tool for dbt

## Installation

```
pip install git+https://github.com/amedia/dbtwiz
```

## Development

```
# where you keep locally checked out repos:
git clone git@github.com:amedia/dbtwiz

# inside the virtual environment of your dbt project:
pip install -e <local-path-to-dbtwiz-repository>
```

## Configuration

The default configuration of _dbtwiz_ will be installed the first time you run it, but you
may want to adjust some settings from the get-go to fit your environment.

### Dark mode
If you're using a dark background colour in your terminal, you should configure _dbtwiz_ to
use bright colours for highlighting in previews and elsewhere to make the text more readable.

Run the following command to switch from default light mode to dark mode:
```shell
$ dbtwiz config theme dark
```

### Preview formatter

By default, _dbtwiz_ uses the command _fmt_ tool to format text in the preview window when
selecting models interactively. Under macOS, the _fmt_ tool won't handle ANSI escape codes,
and unless you have the GNU coreutils version of _fmt_ you will get garbage characters in the
preview window, and should switch to the simple _cat_ command for formatting instead:
```shell
$ dbtwiz config model_info:formatter "cat -s"
```

## Subcommands

- model
    - create
    - inspect
    - from-sql
- source
    - create
- [build](docs/build.md)
- test
- sqlfix
- manifest
- [backfill](docs/backfill.md)
- freshness
- config
- admin
    - orphaned
    - cleandev
    - partition-expiry
