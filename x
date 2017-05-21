#!/usr/bin/env python
from argparse import ArgumentParser
from ConfigParser import ConfigParser
from xclib.cli import Cli, error
import os
import re
import sys

if __name__ == '__main__':
    DEFAULT_OPTIONS = {
        "progressbar": "on",
        "mode": "collapse",
        "projects": "",
        "user": os.getlogin(),
        "cache_dir": os.path.join(os.getenv("HOME"), ".xcute_cache"),
        "conductor_host": "localhost",
        "conductor_port": "5000"
    }
    cp = ConfigParser(defaults=DEFAULT_OPTIONS)

    cfilename = os.path.join(os.getenv("HOME"), ".xcute.conf")
    options = {}
    projects = set()

    try:
        with open(cfilename) as cf:
            try:
                cp.readfp(cf)
                options["progressbar"] = cp.getboolean("main", "progressbar")
                options["mode"] = cp.get("main", "mode")
                options["user"] = cp.get("main", "user")
                options["cache_dir"] = cp.get("main", "cache_dir")
                options["conductor_host"] = cp.get("main", "conductor_host")
                options["conductor_port"] = cp.getint("main", "conductor_port")
                p_names = cp.get("main", "projects")
                if p_names == "":
                    error("you should include at least one project into 'projects' variable in config")
                    sys.exit(1)

                for p_name in re.split(r"\s*,\s*", p_names):
                    projects.add(p_name)

                options["projects"] = list(projects)

            except Exception as e:
                error("invalid configuration file: %s" % e.message)
                sys.exit(1)

    except (OSError, IOError):
        error("no configuration file can be read, can't start with empty project list")
        sys.exit(1)

    except Exception as e:
        error("Error reading configuration: %s" % e.message)
        sys.exit(1)

    parser = ArgumentParser(description="xcute conductor-backed execution tool")
    parser.add_argument("-s", "--stream", dest="mode", action="store_const", const="stream", help="set stream mode")
    parser.add_argument("-c", "--collapse", dest="mode", action="store_const", const="collapse", help="set collapse mode")
    parser.add_argument("-p", "--progressbar", dest="progressbar", action="store_true", help="set progressbar on")
    parser.add_argument("-n", "--no-progressbar", dest="progressbar", action="store_false", help="set progressbar off")
    parser.add_argument("-u", "--user", dest="user", default=os.getlogin(), help="set executer user (default is current terminal user)")
    parser.add_argument('cmd', metavar='command', nargs='?', help='command to execute')
    parser.add_argument('args', metavar='argument', nargs='*', help='command arguments')

    args = parser.parse_args()

    if args.mode:
        options["mode"] = args.mode
    if args.progressbar is not None:
        options["progressbar"] = args.progressbar

    shell = Cli(options)
    if args.cmd:
        arguments = args.cmd
        if args.args:
            arguments += ' ' + ' '.join(args.args)
        shell.onecmd(arguments)
    else:
        shell.cmdloop()