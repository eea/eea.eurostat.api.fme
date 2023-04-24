from dataclasses import dataclass, field
from enum import Enum, unique
from typing import List

@unique
class PreambleSection(Enum):
    SOURCE = 1
    WORKBENCH_SOURCE = 2
    DESTINATION = 3
    WORKBENCH_DESTINATION = 4

@dataclass
class Preamble:
    section: PreambleSection

@dataclass
class Param:
    name: str
    value: any

'''
SOURCE_READER
Syntax: SOURCE_READER <READER NAME> [<PARM NAME> <PARM_VALUE>] [[-]<Schema Keyword> <Schema Macro|Constant Value>]
'''
@dataclass
class SourceReader:
    name: str
    params: List[Param] = field(default_factory=list)


@dataclass
class MetaFile:
    preambles: List[Preamble] # = field(default_factory=list)
    source_reader: SourceReader

if __name__ == '__main__':
    import argparse
    from ruamel.yaml import YAML

    parser = argparse.ArgumentParser(
        prog = 'ProgramName',
        description = 'What the program does',
        epilog = 'Text at the bottom of help')
    parser.add_argument('config')           # positional argument
    #parser.add_argument('-c', '--count')      # option that takes a value
    #parser.add_argument('-v', '--verbose',
    #    action='store_true')  # on/off flag
    args = parser.parse_args()
    print(args)
    yaml=YAML(typ='safe')   # default, if not specfied, is 'rt' (round-trip)
    pkg_yml = yaml.load(open(args.config, 'r'))
    publisher, uid = [pkg_yml.get(k) for k in ['publisher_uid', 'uid']]
    for fmt in pkg_yml.get('package_content', {}).get('formats'):
        fmt_name = '.'.join([publisher, uid, fmt.get('name')]).upper()
        print(fmt_name)
        #print(args.filename, args.count, args.verbose)
        print(PreambleSection.SOURCE)
        source_preamble = Preamble(PreambleSection.SOURCE)
        print(source_preamble)

        source_reader = SourceReader(fmt_name)
        metafile = MetaFile([source_preamble], source_reader)
        print(metafile)

import sys, inspect
def print_classes():
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj):
            print(obj)
