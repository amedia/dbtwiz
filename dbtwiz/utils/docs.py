import inspect
import re
from pathlib import Path
from typing import Annotated, List, get_args, get_origin

import typer
from typer.models import ArgumentInfo, DefaultPlaceholder, OptionInfo

from dbtwiz import main

# Get the directory where this script lives
SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent  # Goes up two levels from dbtwiz/utils/
DOCS_DIR = REPO_ROOT / "docs"
README_PATH = REPO_ROOT / "README.md"


def sanitize_filename(name: str) -> str:
    """Convert any string to a safe filename"""
    if not name or isinstance(name, (int, float, bool)):
        return "command"
    name = re.sub(r"[^\w\-_]", "", str(name).replace(" ", "_"))
    return name.lower() or "command"


def extract_param_info(param) -> dict:
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


def generate_markdown(app_name: str, full_command_path: List[str], command_func):
    """Generate documentation for a single command"""
    DOCS_DIR.mkdir(exist_ok=True)

    full_command_path = [
        command for command in full_command_path if type(command) != DefaultPlaceholder
    ]

    command_path_str = " ".join([app_name] + full_command_path)
    safe_filename = "_".join(
        full_command_path
    ).lower()  # Removed app_name from filename
    output_file = DOCS_DIR / f"{safe_filename}.md"

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

        param_info = extract_param_info(param)

        if param_info["is_option"] and param_info["flags"]:
            flags = [f"`{flag}`" for flag in param_info["flags"]]
            options.append(f"### {', '.join(flags)}\n\n{param_info['help']}\n")
        elif not param_info["is_option"]:
            required_args.append(f"- `{param_info['name']}`: {param_info['help']}")

    markdown = f"# `{command_path_str}`\n\n{description}\n\n"
    if required_args:
        markdown += "## Required arguments\n\n" + "\n".join(required_args) + "\n\n"
    if options:
        markdown += "## Options\n\n" + "\n".join(options)

    # Get examples from decorator if they exist
    example_commands = getattr(command_func, "_command_examples", [])

    # Generate examples section
    examples_section = ""
    if example_commands:
        examples_section = (
            "\n## Examples\n\n" + "\n".join(cmd for cmd in example_commands) + "\n"
        )

    # ... rest of markdown generation ...

    markdown += examples_section

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(markdown)
        print(f"✅ Generated: {output_file.relative_to(REPO_ROOT)}")
    except Exception as e:
        print(f"❌ Failed to write {output_file.relative_to(REPO_ROOT)}: {e}")


def get_group_name(group) -> str:
    """Get the group name with fallbacks"""
    if hasattr(group, "name") and group.name:
        return group.name
    if hasattr(group, "typer_instance") and hasattr(group.typer_instance.info, "name"):
        return group.typer_instance.info.name
    return "group"


def should_have_docs(cmd) -> bool:
    """Determine if a command should have its own documentation file"""
    return hasattr(cmd, "callback") and cmd.callback


def generate_readme_command_list(app: typer.Typer, app_name: str) -> str:
    """Generate a markdown list of all commands with links to their documentation"""
    command_list = []

    def get_clean_command_name(cmd) -> str:
        """Get a clean command name, handling DefaultPlaceholder and other cases"""
        if isinstance(cmd, DefaultPlaceholder):
            return str(cmd.value) if hasattr(cmd, "value") else "command"
        return str(cmd)

    def get_command_description(cmd) -> str:
        """Extract the command description from help or the callback info"""
        if hasattr(cmd, "help") and cmd.help:
            return cmd.help

        if hasattr(cmd, "callback") and cmd.callback and cmd.callback.__doc__:
            return inspect.cleandoc(cmd.callback.__doc__)

        return "No description available."

    def process_command_group(group, current_path: List[str], level: int = 0):
        nonlocal command_list

        # Clean the current path
        cleaned_path = [
            get_clean_command_name(cmd)
            for cmd in current_path
            if not isinstance(cmd, DefaultPlaceholder)
        ]

        # Process commands first
        for cmd in getattr(group, "registered_commands", []):
            if hasattr(cmd, "callback") and cmd.callback:
                cmd_name = get_clean_command_name(cmd.callback.__name__)

                full_path = cleaned_path + [cmd_name]
                safe_filename = "_".join(
                    full_path
                ).lower()  # Removed app_name from filename

                description = get_command_description(cmd)
                indent = "  " * level
                rel_docs_path = f"docs/{safe_filename}.md"
                command_list.append(
                    f"{indent}- [`{cmd_name}`]({rel_docs_path}) - {description}"
                )

        # Then process groups (subcommands)
        for subgroup in getattr(group, "registered_groups", []):
            if hasattr(subgroup, "typer_instance"):
                group_name = get_group_name(subgroup)
                if group_name is None or isinstance(group_name, DefaultPlaceholder):
                    process_command_group(subgroup.typer_instance, cleaned_path, level)
                    continue

                full_path = cleaned_path + [group_name]
                indent = "  " * level
                description = get_command_description(subgroup)
                safe_filename = "_".join(
                    full_path
                ).lower()  # Removed app_name from filename
                rel_docs_path = f"docs/{safe_filename}.md"

                if should_have_docs(subgroup):
                    command_list.append(
                        f"{indent}- [`{group_name}`]({rel_docs_path}) - {description}"
                    )
                else:
                    command_list.append(f"{indent}- `{group_name}` - {description}")

                process_command_group(subgroup.typer_instance, full_path, level + 1)

    process_command_group(app, [])
    return "\n".join(command_list)


def update_readme(app: typer.Typer, app_name: str):
    """Update README.md with the command list between the special comments"""
    if not README_PATH.exists():
        print(f"❌ README.md not found at {README_PATH.relative_to(REPO_ROOT)}")
        return

    try:
        with open(README_PATH, "r", encoding="utf-8") as f:
            content = f.read()

        start_marker = "[comment]: <> (START ACCESS CONFIG)"
        end_marker = "[comment]: <> (END ACCESS CONFIG)"

        start_pos = content.find(start_marker)
        end_pos = content.find(end_marker)

        if start_pos == -1 or end_pos == -1:
            print("❌ Could not find START/END ACCESS CONFIG markers in README.md")
            return

        before_content = content[: start_pos + len(start_marker)]
        after_content = content[end_pos:]

        command_list = generate_readme_command_list(app, app_name)
        new_content = f"{before_content}\n\n{command_list}\n\n{after_content}"

        with open(README_PATH, "w", encoding="utf-8") as f:
            f.write(new_content)

        print(f"✅ Updated command list in {README_PATH.relative_to(REPO_ROOT)}")
    except Exception as e:
        print(f"❌ Failed to update {README_PATH.relative_to(REPO_ROOT)}: {e}")


def process_commands(
    app: typer.Typer, app_name: str, parent_commands: List[str] = None
):
    """Process all commands recursively"""
    parent_commands = parent_commands or []

    # Process commands
    for cmd in getattr(app, "registered_commands", []):
        if hasattr(cmd, "callback") and cmd.callback:
            cmd_name = cmd.callback.__name__
            generate_markdown(app_name, parent_commands + [cmd_name], cmd.callback)

    # Process groups
    for group in getattr(app, "registered_groups", []):
        if hasattr(group, "typer_instance"):
            group_name = get_group_name(group)
            process_commands(
                group.typer_instance, app_name, parent_commands + [group_name]
            )


def update_docs():
    process_commands(main.app, "dbtwiz")
    update_readme(main.app, "dbtwiz")
