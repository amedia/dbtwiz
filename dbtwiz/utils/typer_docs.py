import inspect
from pathlib import Path
from typing import Annotated, List, get_args, get_origin

import typer
from typer.models import ArgumentInfo, DefaultPlaceholder, OptionInfo


def _extract_param_info(param) -> dict:
    """Extract all parameter information including both long and short flags"""
    param_info = {
        "name": param.name,
        "help": "No description available",
        "required": True,
        "flags": [],
        "is_option": False,
    }

    # Get the Typer parameter object (could be in default or in Annotated)
    typer_param = None

    # First check if it's an Annotated parameter
    if get_origin(param.annotation) is Annotated:
        for extra in get_args(param.annotation)[1:]:
            if isinstance(extra, (OptionInfo, ArgumentInfo)):
                typer_param = extra
                break

    # If not Annotated, check the default value
    if typer_param is None and isinstance(param.default, (OptionInfo, ArgumentInfo)):
        typer_param = param.default

    # If we found a Typer parameter object, extract its properties
    if typer_param:
        # Safely get param_decls (handle None case)
        param_decls = getattr(typer_param, "param_decls", None)
        flags = list(param_decls) if param_decls is not None else []

        param_info.update(
            {
                "help": getattr(typer_param, "help", param_info["help"]),
                "required": not getattr(typer_param, "required", True),
                "flags": flags,  # Now safely converted to list
                "is_option": isinstance(typer_param, OptionInfo),
            }
        )

        # For options, include both the long name and any short names
        if param_info["is_option"]:
            # Add the parameter name as a long flag if it's not already in flags
            long_flag = f"--{param.name.replace('_', '-')}"
            if long_flag not in param_info["flags"]:
                param_info["flags"].insert(0, long_flag)

    # Handle required arguments that aren't options
    if param.default == inspect.Parameter.empty and not param_info["is_option"]:
        param_info["required"] = True

    return param_info


def _get_decorator_docs(command_func, decorator_name, docs_header) -> str:
    decorator_value = getattr(command_func, decorator_name, [])

    # Generate docs section
    section_markdown = ""
    if decorator_value:
        section_markdown = (
            (f"\n## {docs_header}\n\n" if docs_header else "")
            + "\n".join(val for val in decorator_value)
            + "\n"
        )

    return section_markdown


def _generate_markdown(
    app_name: str,
    full_command_path: List[str],
    command_func,
    repo_root: Path,
    docs_dir: Path,
):
    """Generate documentation for a single command"""
    docs_dir.mkdir(exist_ok=True)

    full_command_path = [
        command
        for command in full_command_path
        if not isinstance(command, DefaultPlaceholder)
    ]

    # command_path_str = " ".join([app_name] + full_command_path)
    command_path_str = " ".join(
        [app_name] + [cmd.replace("_", "-") for cmd in full_command_path]
    )
    safe_filename = "_".join(
        full_command_path
    ).lower()  # Removed app_name from filename
    output_file = docs_dir / f"{safe_filename}.md"

    description = (
        inspect.cleandoc(command_func.__doc__)
        if command_func.__doc__
        else "No description available."
    )

    sig = inspect.signature(command_func)
    required_args = []
    options = []

    for name, param in sig.parameters.items():
        if name == "return":
            continue

        param_info = _extract_param_info(param)

        if param_info["is_option"] and param_info["flags"]:
            flags = [f"`{flag}`" for flag in param_info["flags"]]
            options.append(f"### {', '.join(flags)}\n\n{param_info['help']}\n")
        elif not param_info["is_option"]:
            required_args.append(f"- `{param_info['name']}`: {param_info['help']}")

    markdown = f"# `{command_path_str}`\n\n{description}\n\n"

    markdown += _get_decorator_docs(command_func, "_command_description", None)

    if required_args:
        markdown += "## Required arguments\n\n" + "\n".join(required_args) + "\n\n"
    if options:
        markdown += "## Options\n\n" + "\n".join(options)

    markdown += _get_decorator_docs(command_func, "_command_examples", "Examples")

    # Check for changes before writing file
    if output_file.exists():
        with open(output_file, "r", encoding="utf-8") as f:
            if f.read() == markdown:
                return

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(markdown)
        print(f"[+] Generated: {output_file.relative_to(repo_root)}")
    except Exception as e:
        print(f"[x] Failed to write {output_file.relative_to(repo_root)}: {e}")


def _get_group_name(group) -> str:
    """Get the group name with fallbacks"""
    if hasattr(group, "name") and group.name:
        return group.name
    if hasattr(group, "typer_instance") and hasattr(group.typer_instance.info, "name"):
        return group.typer_instance.info.name
    return "group"


def _should_have_docs(cmd) -> bool:
    """Determine if a command should have its own documentation file"""
    return hasattr(cmd, "callback") and cmd.callback


def _get_clean_command_name(cmd) -> str:
    """Get a clean command name, handling DefaultPlaceholder and other cases"""
    if isinstance(cmd, DefaultPlaceholder):
        return str(cmd.value) if hasattr(cmd, "value") else "command"
    return str(cmd)


def _get_command_description(cmd) -> str:
    """Extract the command description from help or the callback info"""
    if hasattr(cmd, "help") and cmd.help:
        return cmd.help

    if hasattr(cmd, "callback") and cmd.callback and cmd.callback.__doc__:
        return inspect.cleandoc(cmd.callback.__doc__)

    return "No description available."


def _process_command_group(group, current_path: List[str], level: int = 0) -> List[str]:
    """Process a command group and return a list of command entries"""
    command_list = []

    # Clean the current path
    cleaned_path = [
        _get_clean_command_name(cmd)
        for cmd in current_path
        if not isinstance(cmd, DefaultPlaceholder)
    ]

    # Process commands first
    for cmd in getattr(group, "registered_commands", []):
        if hasattr(cmd, "callback") and cmd.callback:
            cmd_name = _get_clean_command_name(cmd.callback.__name__)

            full_path = cleaned_path + [cmd_name]
            safe_filename = "_".join(full_path).lower()
            display_name = cmd_name.replace("_", "-")
            description = _get_command_description(cmd)
            indent = "  " * level
            rel_docs_path = f"docs/{safe_filename}.md"
            command_list.append(
                f"{indent}- [`{display_name}`]({rel_docs_path}) - {description}"
            )

    # Then process groups (subcommands)
    for subgroup in getattr(group, "registered_groups", []):
        if hasattr(subgroup, "typer_instance"):
            group_name = _get_group_name(subgroup)
            if group_name is None or isinstance(group_name, DefaultPlaceholder):
                command_list.extend(
                    _process_command_group(subgroup.typer_instance, cleaned_path, level)
                )
                continue

            full_path = cleaned_path + [group_name]
            indent = "  " * level
            description = _get_command_description(subgroup)
            safe_filename = "_".join(full_path).lower()
            rel_docs_path = f"docs/{safe_filename}.md"

            if _should_have_docs(subgroup):
                command_list.append(
                    f"{indent}- [`{group_name}`]({rel_docs_path}) - {description}"
                )
            else:
                command_list.append(f"{indent}- `{group_name}` - {description}")

            command_list.extend(
                _process_command_group(subgroup.typer_instance, full_path, level + 1)
            )

    return command_list


def _generate_readme_command_list(app: typer.Typer, app_name: str) -> str:
    """Generate a markdown list of all commands with links to their documentation"""
    command_list = _process_command_group(app, [])
    return "\n".join(command_list)


def _update_readme(app: typer.Typer, app_name: str, repo_root: Path, readme_path: Path):
    """Update README.md with the command list between the special comments"""
    if not readme_path.exists():
        print(f"[x] README.md not found at {readme_path.relative_to(repo_root)}")
        return

    try:
        with open(readme_path, "r", encoding="utf-8") as f:
            content = f.read()

        start_marker = "[comment]: <> (START COMMAND DOCS)"
        end_marker = "[comment]: <> (END COMMAND DOCS)"

        start_pos = content.find(start_marker)
        end_pos = content.find(end_marker)

        if start_pos == -1 or end_pos == -1:
            print("[x] Couldn't find START/END COMMAND DOCS markers in README.md")
            return

        before_content = content[: start_pos + len(start_marker)]
        after_content = content[end_pos:]

        command_list = _generate_readme_command_list(app, app_name)
        new_content = f"{before_content}\n\n{command_list}\n\n{after_content}"

        # Check for changes before writing file
        if content != new_content:
            with open(readme_path, "w", encoding="utf-8") as f:
                f.write(new_content)
            print(f"[+] Updated command list in {readme_path.relative_to(repo_root)}")

    except Exception as e:
        print(f"[x] Failed to update {readme_path.relative_to(repo_root)}: {e}")


def _process_commands(
    app: typer.Typer,
    app_name: str,
    repo_root: Path,
    docs_dir: Path,
    parent_commands: List[str] = None,
):
    """Process all commands recursively"""
    parent_commands = parent_commands or []

    # Process commands
    for cmd in getattr(app, "registered_commands", []):
        if hasattr(cmd, "callback") and cmd.callback:
            cmd_name = cmd.callback.__name__
            _generate_markdown(
                app_name=app_name,
                full_command_path=parent_commands + [cmd_name],
                command_func=cmd.callback,
                repo_root=repo_root,
                docs_dir=docs_dir,
            )

    # Process groups
    for group in getattr(app, "registered_groups", []):
        if hasattr(group, "typer_instance"):
            group_name = _get_group_name(group)
            _process_commands(
                app=group.typer_instance,
                app_name=app_name,
                repo_root=repo_root,
                docs_dir=docs_dir,
                parent_commands=parent_commands + [group_name],
            )


def generate(
    app: typer.Typer, app_name: str, repo_root: Path, docs_dir: Path, readme_path: Path
):
    """
    Identifies commands in the given typer app, and for each command and subcommand,
    creates markdown files documenting the command in the given docs_dir.

    In addition, all the commands are added to `README.md` in the given readme path
    between the following two markers, should they exist:
    ```
    [comment]: <> (START COMMAND DOCS)
    [comment]: <> (END COMMAND DOCS)`
    ```
    """
    _process_commands(
        app=app, app_name=app_name, repo_root=repo_root, docs_dir=docs_dir
    )
    _update_readme(
        app=app, app_name=app_name, repo_root=repo_root, readme_path=readme_path
    )


if __name__ == "__main__":
    # Specific config for dbtwiz package
    # Use absolute imports when running as script
    import sys
    from pathlib import Path

    # Add the parent directory to sys.path to allow imports
    script_dir = Path(__file__).parent.resolve()
    repo_root = script_dir.parent.parent  # Goes up two levels from dbtwiz/utils/
    sys.path.insert(0, str(repo_root))

    from dbtwiz.cli.main import app

    docs_dir = repo_root / "docs"
    readme_path = repo_root / "README.md"

    generate(
        app=app,
        app_name="dbtwiz",
        repo_root=repo_root,
        docs_dir=docs_dir,
        readme_path=readme_path,
    )
