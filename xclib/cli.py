import sys
import gnureadline as readline
sys.modules["readline"] = readline
readline.parse_and_bind("tab: complete")

import cmd
import fcntl, termios, struct, os
from termcolor import cprint, colored
from xclib.conductor import Conductor
from xclib.conductor.models import Datacenter, Project, Host, Group



def terminal_size():
    h, w, hp, wp = struct.unpack('HHHH',
        fcntl.ioctl(0, termios.TIOCGWINSZ,
        struct.pack('HHHH', 0, 0, 0, 0)))
    return w, h


def error(msg):
    return cprint("ERROR: %s" % msg, "red")


def warn(msg):
    return cprint("WARNING: %s" % msg, "grey")


class Cli(cmd.Cmd):

    MODES = ("collapse", "stream")
    DEFAULT_MODE = "collapse"
    DEFAULT_OPTIONS = {
        "progressgbar": True
    }
    HISTORY_FILE = os.path.join(os.getenv("HOME"), ".xcute_history")

    def __init__(self, options={}):
        cmd.Cmd.__init__(self)
        self.conductor = Conductor(options["projects"],
                                   host=options["conductor_host"],
                                   port=options["conductor_port"],
                                   cache_dir=options["cache_dir"])

        self.user = options.get("user") or os.getlogin()
        self.progressbar = options.get("progressbar") or self.DEFAULT_OPTIONS["progressgbar"]
        self.finished = False
        if "mode" in options:
            if not options["mode"] in self.MODES:
                error("invalid mode '%s'. use 'stream' or 'collapse'" % options["mode"])
                self.mode = self.DEFAULT_MODE
            else:
                self.mode = options["mode"]
        else:
            self.mode = self.DEFAULT_MODE

    @property
    def prompt(self):
        if self.mode == "collapse":
            mode = colored("[Collapse]", "green")
        else:
            mode = colored("[Stream]", "yellow")
        return "%s %s> " % (mode, colored(self.user, "blue", attrs=["bold"]))

    def postcmd(self, stop, line):
        return self.finished

    def cmdloop(self, intro=None):
        try:
            cmd.Cmd.cmdloop(self)
        except KeyboardInterrupt:
            print
            self.cmdloop()

    def preloop(self):
        delims = set(readline.get_completer_delims())
        delims.remove('%')
        delims.remove('*')
        delims.remove('-')
        delims.remove('/')
        readline.set_completer_delims(''.join(delims))

        try:
          readline.read_history_file(self.HISTORY_FILE)
        except (OSError, IOError), e:
          warn("Can't read history file: %s" % e.message)

    def postloop(self):
        try:
            readline.write_history_file(self.HISTORY_FILE)
        except (OSError, IOError) as e:
            warn("Can't write history file: %s" % e.message)

    def __on_off_completion(self, text):
        return [x for x in ['on', 'off'] if x.startswith(text.lower())]

    def print_option(self, name):
        value = getattr(self, name)
        if value:
            cprint(name.capitalize() + ": on", "green")
        else:
            cprint(name.capitalize() + ": off", "red")

    def do_progressbar(self, args):
        """progressbar:\n switch progressbar <on|off>"""
        if args:
            mode = args.split()[0].lower()
            if mode not in ("on", "off"):
                print "Usage: progressbar [on|off]"
                return
            if mode == "on":
                self.progressbar = True
            else:
                self.progressbar = False
        self.print_option("progressbar")

    def complete_progressbar(self, text, line, begidx, endidx):
        return self.__on_off_completion(text)

    def do_EOF(self, args):
        """exit:\n  exits program"""
        print
        self.finished = True

    def do_exit(self, args):
        """exit:\n  exits program"""
        self.finished = True

    def do_mode(self, args):
        """mode:\n  set exec output mode to collapse/stream"""
        if args:
            mode = args.split()[0]
        else:
            mode = Cli.DEFAULT_MODE
        if not mode in Cli.MODES:
            error("Invalid mode: %s, use 'collapse' or 'stream'" % mode)
            return
        self.mode = mode

    def complete_mode(self, text, line, begidx, endidx):
        return [x for x in self.MODES if x.startswith(text)]

    def do_hostlist(self, args):
        """hostlist:\n  resolve conductor expression to host list"""
        args = args.split()
        if len(args) != 1:
            cprint("Usage: hostlist <conductor expression>", "red")
            return

        expr = args[0]
        hosts = list(self.conductor.resolve(expr))
        if len(hosts) == 0:
            cprint("Empty list", "red")
            return

        hosts.sort()
        hrlen = max([len(x) for x in hosts])
        if hrlen < 10 + len(expr): hrlen = 10 + len(expr)
        hr = colored("=" * hrlen, "green")

        print hr
        print "Hostlist: " + expr
        print hr
        for host in hosts:
            print host

    def __completion_argnum(self, line, endidx):
        return len(line[:endidx].split()) - 2

    def complete_hostlist(self, text, line, begidx, endidx):
        return self.complete_exec(text, line, begidx, endidx)

    def complete_exec(self, text, line, begidx, endidx):
        if self.__completion_argnum(line, endidx) != 0:
            return []

        if line[begidx-1] == "@":
            dcpref = text
            datacenters = self.conductor.autocompleters[Datacenter].complete(dcpref)
            return list(datacenters)

        prefix = ''

        if text.startswith('-'):
            prefix += '-'
            text = text[1:]

        if text.startswith('%'):
            groups = self.conductor.autocompleters[Group].complete(text[1:])
            return [prefix + "%" + g for g in groups]
        elif text.startswith('*'):
            projects = self.conductor.autocompleters[Project].complete(text[1:])
            return [prefix + "*" + p for p in projects]
        else:
            hosts = self.conductor.autocompleters[Host].complete(text)
            return [prefix + h for h in hosts]