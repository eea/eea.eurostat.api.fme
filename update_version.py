import subprocess
import re

from ruamel.yaml import YAML
import tomli
import tomli_w


VERSION_BRANCH_PATTERN = re.compile(r"v_(\d+)\.(\d+)\.(\d+)")
SETUP_VERSION_LINE_PATTERN = re.compile(r'^( +)version="(\d+\.\d+\.\d+)",$')


def git_branch():
    """Return the current git branch name."""
    cmdline = ["git", "branch", "--no-color", "--show-current"]
    process = subprocess.run(cmdline, capture_output=True, check=True, encoding="utf8")
    return process.stdout.strip()


def update_setup_py(new_version):
    """Update the version field in setup.py if it differs from new_version."""
    file_path = r"python\fme-eurostat\setup.py"
    with open(file_path, "r", encoding="utf8") as file_in:
        lines = file_in.read().split("\n")

    update_line = None
    for line_number, line_text in enumerate(lines):
        match = SETUP_VERSION_LINE_PATTERN.match(line_text)
        if match:
            old_version = match.group(2)
            if old_version != new_version:
                update_line = (line_number, f'{match.group(1)}version="{new_version}",')
            break

    if update_line:
        line_number, new_value = update_line
        print("setup.py - replacing line no.", line_number, lines[line_number], "->", new_value)
        lines[line_number] = new_value
        with open(file_path, "w", encoding="utf8") as file_out:
            file_out.write("\n".join(lines))


def update_package_yml(new_version):
    """Update the package version in package.yml if needed."""
    yaml = YAML(typ="safe")
    with open("package.yml", "r") as file_in:
        package_info = yaml.load(file_in)

    old_version = package_info.get("version")
    if old_version != new_version:
        print("package.yml - replacing version info", old_version, "->", new_version)
        package_info["version"] = new_version
        with open("package.yml", "w") as file_out:
            yaml.dump(package_info, file_out)


def update_helpdoc_title(new_version):
    """Update docs/help/book.toml title suffix to include the current version."""
    with open(r"docs\help\book.toml", "rb") as file_in:
        book_info = tomli.load(file_in)

    old_title = book_info.get("book", {}).get("title")
    # Strip any existing [x.y.z] suffix before appending the target version.
    base_title = re.sub(r"\[\d+\.\d+\.\d+\]", "", old_title).strip()
    new_title = f"{base_title} [{new_version}]"

    if old_title != new_title:
        book_info["book"]["title"] = new_title
        print("book.toml", old_title, "->", new_title)
        with open(r"docs\help\book.toml", "wb") as file_out:
            tomli_w.dump(book_info, file_out)


if __name__ == "__main__":
    branch_name = git_branch()
    # Release versions are derived from the branch name, e.g. v_1.2.3.
    match = VERSION_BRANCH_PATTERN.match(branch_name)
    if not match:
        raise Exception(
            "Please use a branch name that uses naming convention v_x.y.z "
            f"when creating a distribution, current branch name is `{branch_name}`"
        )

    version = ".".join(match.groups())
    print(version)
    update_setup_py(version)
    update_package_yml(version)
    update_helpdoc_title(version)

