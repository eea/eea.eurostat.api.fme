import subprocess
from ruamel.yaml import YAML
import tomli
import tomli_w
import re

def git_branch():
    cmdline = ['git', 'branch', '--no-color', '--show-current']
    p = subprocess.run(cmdline, capture_output=True, check=True, encoding='utf8')
    return p.stdout.strip()

def update_setup_py(new_version):
    line_nbr = -1
    pat = re.compile(r'^( +)version="(\d+\.\d+\.\d+)",$')
    fp = r'python\fme-eurostat\setup.py'
    lines = []
    with open(fp, 'r', encoding='utf8') as fin:
        lines = fin.read().split('\n')
    update_line = None
    for i,s in enumerate(lines):
        m = pat.match(s)
        if m:
            line_nbr = i
            old_version = m.group(2)
            if not old_version==new_version:
                update_line = (i, f'{m.group(1)}version="{new_version}",')
            break
    if update_line:
        line_nbr, new_value = update_line
        print('setup.py - replacing line no.', line_nbr, lines[line_nbr], '->', new_value)
        lines[line_nbr] = new_value
        with open(fp, 'w', encoding='utf8') as fout:
            fout.write('\n'.join(lines))

def update_package_yml(new_version):
    yaml=YAML(typ='safe')
    pkg_info = dict()
    with open('package.yml', 'r') as fin:
        pkg_info = yaml.load(fin)
    old_version = pkg_info.get('version')
    if not old_version == new_version:
        print('package.yml - replacing version info', old_version, '->', new_version)
        pkg_info['version'] = new_version
        with open('package.yml', 'w') as fout:
            yaml.dump(pkg_info, fout)
        
def update_helpdoc_title(new_version):
    book_info = dict()
    with open(r'docs\help\book.toml', 'rb') as fin:
        book_info = tomli.load(fin)
    old_title = book_info.get('book', {}).get('title')
    base_title = re.sub(r'\[\d+\.\d+\.\d+\]', '', old_title).strip()
    new_title = f'{base_title} [{new_version}]'
    if not old_title == new_title:
        book_info['book']['title'] = new_title
        print('book.toml', old_title, '->', new_title)
        with open(r'doc\help\book.toml', 'wb') as fout:
            tomli_w.dump(book_info, fout)

if __name__ == '__main__':
    branch_name = git_branch()
    m = re.match('v_(\d+)\.(\d+)\.(\d+)', branch_name)
    if not m:
        raise Exception(f'Please use a branch name that uses naming convention v_x.y.z when creating a distribution, current branch name is `{branch_name}`')
    version = '.'.join(m.groups())
    print(version)
    update_setup_py(version)
    update_package_yml(version)
    update_helpdoc_title(version)
    
