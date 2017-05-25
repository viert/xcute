from gevent import Greenlet
from gevent.select import select
from gevent.pool import Pool
from gevent.subprocess import Popen, PIPE
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
    cprint("ERROR: %s" % msg, "red")


def warn(msg):
    cprint("WARNING: %s" % msg, "yellow")


def export_print(msg):
    cprint(msg, "yellow")


def aligned(message, align_len):
    message = "=" * 6 + " " + message + " "
    return message + "=" * (align_len - len(message))


class Cli(cmd.Cmd):

    MODES = ("collapse", "stream", "serial")
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
                                   cache_dir=options["cache_dir"],
                                   print_func=export_print)
        self.ssh_threads = options["ssh_threads"]
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
        elif self.mode == "stream":
            mode = colored("[Stream]", "yellow")
        elif self.mode == "serial":
            mode = colored("[Serial]", "cyan")

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
        for d in "%*-/":
            try:
                delims.remove(d)
            except KeyError:
                pass
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
        argnum = len(line[:endidx].split(" ")) - 2
        return argnum

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

    def complete_ssh(self, text, line, begidx, endidx):
        return self.complete_exec(text, line, begidx, endidx)

    def do_ssh(self, args):
        """ssh:\n connect to host(s) via ssh"""
        hosts = self.conductor.resolve(args.split()[0])
        for host in hosts:
            cprint("=== ssh %s@%s ===" % (self.user, host), "green")
            command = "ssh -l %s %s" % (self.user, host)
            os.system(command)

    def do_stream(self, args):
        """stream:\n  shortcut to 'mode stream'"""
        return self.do_mode("stream")

    def do_collapse(self, args):
        """collapse:\n  shortcut to 'mode collapse'"""
        return self.do_mode("collapse")

    def do_serial(self, args):
        """serial:\n  shortcut to 'mode serial'"""
        return self.do_mode("serial")

    def do_exec(self, args):
        expr, cmd = args.split(None, 1)
        hosts = self.conductor.resolve(expr)
        if len(hosts) == 0:
            error("Empty hostlist")
            return

        if self.mode == "stream":
            self.run_stream(hosts, cmd)
        elif self.mode == "collapse":
            self.run_collapse(hosts, cmd)
        elif self.mode == "serial":
            self.run_serial(hosts, cmd)

    def run_serial(self, hosts, cmd):
        codes = {"total": 0, "error": 0, "success": 0}
        align_len = len(max(hosts, key=len)) + len(self.user) + len(cmd) + 24

        for host in hosts:
            msg = "ssh %s@%s \"%s\"" % (self.user, host, cmd)
            cprint(aligned(msg, align_len), "blue", attrs=["bold"])
            code = os.system("ssh -l %s %s \"%s\"" % (self.user, host, cmd))
            if code == 0:
                codes["success"] += 1
            else:
                codes["error"] += 1
            codes["total"] += 1

        self.print_exec_results(codes)

    def print_exec_results(self, codes):
        msg = " Hosts processed: %d, success: %d, error: %d    " % (codes["total"], codes["success"], codes["error"])
        hr = "=" * len(msg)
        cprint(hr, "green")
        cprint(msg, "green")
        cprint(hr, "green")

    def run_stream(self, hosts, cmd):
        codes = {"total": 0, "error": 0, "success": 0}
        def worker(host, cmd):
            p = Popen(["ssh", "-l", self.user, host, cmd], stdout=PIPE, stderr=PIPE)
            while True:
                outs, _, _ = select([p.stdout, p.stderr], [], [])
                if p.stdout in outs:
                    outline = p.stdout.readline()
                else:
                    outline = ""
                if p.stderr in outs:
                    errline = p.stderr.readline()
                else:
                    errline = ""

                if outline == "" and errline == "" and p.poll() is not None:
                    break

                if outline != "":
                    print "%s: %s" % (colored(host, "blue", attrs=["bold"]), outline.strip())
                if errline != "":
                    print "%s: %s" % (colored(host, "blue", attrs=["bold"]), colored(errline.strip(), "red"))
            if p.poll() == 0:
                codes["success"] += 1
            else:
                codes["error"] += 1
            codes["total"] += 1

        pool = Pool(self.ssh_threads)
        for host in hosts:
            pool.start(Greenlet(worker, host, cmd))
        pool.join()
        self.print_exec_results(codes)

    def run_collapse(self, hosts, cmd):
        error("Collapse mode has not been implemented yet")

    def do_user(self, args):
        """user:\n  set user"""
        username = args.split()[0]
        self.user = username