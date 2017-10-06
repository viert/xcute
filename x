#!/usr/bin/env python
from argparse import ArgumentParser
from ConfigParser import ConfigParser
from xclib.cli import Cli, error
import os, pwd
import re
import sys

# Docker hack
os.getlogin = lambda: pwd.getpwuid(os.getuid())[0]

DEFAULT_CONFIG = """
[main]
# Conductor configuration
conductor_host = {conductor_host}
conductor_port = 80
projects = {project_list}

# Executer
user = {user}
ssh_threads = 50
ping_count = 5
default_remote_dir = /tmp
""".format(conductor_host=os.environ.get('CONDUCTOR_HOST', 'localhost'),
           project_list=os.environ.get('PROJECT_LIST', ""),
           user=os.environ.get('CONDUCTOR_USER', os.getlogin()))

if __name__ == '__main__':
    DEFAULT_OPTIONS = {
        "progressbar": "on",
        "mode": "collapse",
        "projects": "",
        "user": os.getlogin(),
        "cache_dir": os.path.join(os.getenv("HOME"), ".xcute_cache"),
        "conductor_host": "localhost",
        "conductor_port": "80",
        "ssh_threads": "50",
        "ping_count": "5",
        "default_remote_dir": "/tmp",
        "use_recursive_fields": "off"
    }
    cp = ConfigParser(defaults=DEFAULT_OPTIONS)

    cfilename = os.path.join(os.getenv("HOME"), ".xcute.conf")
    options = {}
    projects = set()

    while True:
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
                    options["ssh_threads"] = cp.getint("main", "ssh_threads")
                    options["ping_count"] = cp.getint("main", "ping_count")
                    options["default_remote_dir"] = cp.get("main", "default_remote_dir")
                    options["use_recursive_fields"] = cp.getboolean("main", "use_recursive_fields")
                    p_names = cp.get("main", "projects")

                    for p_name in re.split(r"\s*,\s*", p_names):
                        projects.add(p_name)

                    options["projects"] = list(projects)
                    break

                except Exception as e:
                    error("invalid configuration file: %s" % e.message)
                    sys.exit(1)

        except (OSError, IOError):
            with open(cfilename, "w") as cf:
                cf.write(DEFAULT_CONFIG)
                continue

        except Exception as e:
            error("Error reading configuration: %s" % e.message)
            sys.exit(1)

    parser = ArgumentParser(description="xcute conductor-backed execution tool")
    parser.add_argument("-s", "--stream", dest="mode", action="store_const", const="stream", help="set stream mode")
    parser.add_argument("-c", "--collapse", dest="mode", action="store_const", const="collapse",
                        help="set collapse mode")
    parser.add_argument("-p", "--progressbar", dest="progressbar", action="store_true", help="set progressbar on")
    parser.add_argument("-n", "--no-progressbar", dest="progressbar", action="store_false", help="set progressbar off")
    parser.add_argument("-u", "--user", dest="user", default=os.getlogin(),
                        help="set executer user (default is current terminal user)")
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
        shell.set_one_command_mode(True)
        shell.onecmd(arguments)
    else:
        shell.cmdloop()
