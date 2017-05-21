import fcntl, termios, struct, os, readline
from cmd import Cmd
from termcolor import cprint, colored
from xclib.conductor import Conductor


def terminal_size():
    h, w, hp, wp = struct.unpack('HHHH',
        fcntl.ioctl(0, termios.TIOCGWINSZ,
        struct.pack('HHHH', 0, 0, 0, 0)))
    return w, h


def error(msg):
    return cprint("ERROR: %s" % msg, "red")

def warn(msg):
    return cprint("WARNING: %s" % msg, "yellow")

class Cli(Cmd):

    MODES = ("collapse", "stream")
    DEFAULT_MODE = "collapse"
    DEFAULT_OPTIONS = {
        "progressgbar": True
    }
    HISTORY_FILE = os.path.join(os.getenv("HOME"), ".xcute_history")

    def __init__(self, options={}):
        Cmd.__init__(self)

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
            Cmd.cmdloop(self)
        except KeyboardInterrupt:
            print
            self.cmdloop()

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

