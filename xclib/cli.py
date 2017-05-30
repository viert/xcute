from gevent import Greenlet
from gevent.select import select
from gevent.pool import Pool
from gevent.subprocess import Popen, PIPE
from collections import defaultdict
from termcolor import cprint, colored
from xclib.conductor import Conductor
from xclib.conductor.models import Datacenter, Project, Host, Group
import sys, fcntl, termios, struct, os, cmd
import gnureadline as readline
sys.modules["readline"] = readline
readline.parse_and_bind("tab: complete")


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

    MODES = ("collapse", "parallel", "serial")
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
        self.alias_scripts = {}
        if "mode" in options:
            if not options["mode"] in self.MODES:
                error("invalid mode '%s'. use 'parallel', 'collapse' or 'serial" % options["mode"])
                self.mode = self.DEFAULT_MODE
            else:
                self.mode = options["mode"]
        else:
            self.mode = self.DEFAULT_MODE

    @property
    def prompt(self):
        if self.mode == "collapse":
            mode = colored("[Collapse]", "green")
        elif self.mode == "parallel":
            mode = colored("[Parallel]", "yellow")
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
        """mode:\n  set exec output mode to collapse/serial/parallel"""
        if args:
            mode = args.split()[0]
        else:
            mode = Cli.DEFAULT_MODE
        if not mode in Cli.MODES:
            error("Invalid mode: %s, use 'collapse', 'serial' or 'parallel'" % mode)
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

    def complete_p_exec(self, text, line, begidx, endidx):
        return self.complete_exec(text, line, begidx, endidx)

    def complete_c_exec(self, text, line, begidx, endidx):
        return self.complete_exec(text, line, begidx, endidx)

    def complete_s_exec(self, text, line, begidx, endidx):
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

    def do_parallel(self, args):
        """parallel:\n  shortcut to 'mode parallel'"""
        return self.do_mode("parallel")

    def do_collapse(self, args):
        """collapse:\n  shortcut to 'mode collapse'"""
        return self.do_mode("collapse")

    def do_serial(self, args):
        """serial:\n  shortcut to 'mode serial'"""
        return self.do_mode("serial")

    def __extract_exec_args(self, args):
        try:
            expr, cmd = args.split(None, 1)
        except ValueError:
            error("Usage: <exec> <expression> <cmd>")
            return [], args
        hosts = self.conductor.resolve(expr)
        if len(hosts) == 0:
            error("Empty hostlist")
        return hosts, cmd

    def do_exec(self, args):
        hosts, cmd = self.__extract_exec_args(args)
        if len(hosts) == 0:
            return

        if self.mode == "parallel":
            self.run_parallel(hosts, cmd)
        elif self.mode == "collapse":
            self.run_collapse(hosts, cmd)
        elif self.mode == "serial":
            self.run_serial(hosts, cmd)

    def do_p_exec(self, args):
        """p_exec:\n force exec in parallel mode"""
        hosts, cmd = self.__extract_exec_args(args)
        if len(hosts) == 0:
            return
        self.run_parallel(hosts, cmd)

    def do_s_exec(self, args):
        """s_exec:\n force exec in serial mode"""
        hosts, cmd = self.__extract_exec_args(args)
        if len(hosts) == 0:
            return
        self.run_serial(hosts, cmd)

    def do_c_exec(self, args):
        """c_exec:\n force exec in collapse mode"""
        hosts, cmd = self.__extract_exec_args(args)
        if len(hosts) == 0:
            return
        self.run_collapse(hosts, cmd)


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

    def run_parallel(self, hosts, cmd):
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
        progress = None
        if self.progressbar:
            from progressbar import ProgressBar, Percentage, Bar, ETA, FileTransferSpeed
            progress = ProgressBar(
                widgets=["Running: ", Percentage(), ' ', Bar(marker='.'), ' ', ETA(), ' ', FileTransferSpeed()],
                maxval=len(hosts))

        codes = {"total": 0, "error": 0, "success": 0}
        outputs = defaultdict(list)
        def worker(host, cmd):
            p = Popen(["ssh", "-l", self.user, host, cmd], stdout=PIPE, stderr=PIPE)
            o, e = p.communicate()
            outputs[o].append(host)
            if p.poll() == 0:
                codes["success"] += 1
            else:
                codes["error"] += 1
            codes["total"] += 1
            if self.progressbar:
                progress.update(codes["total"])

        pool = Pool(self.ssh_threads)
        if self.progressbar:
            progress.start()
        for host in hosts:
            pool.start(Greenlet(worker, host, cmd))
        pool.join()
        if self.progressbar:
            progress.finish()
        self.print_exec_results(codes)
        print
        for output, hosts in outputs.items():
            msg = " %s    " % ', '.join(hosts)
            table_width = min([len(msg) + 2, terminal_size()[0]])
            cprint("=" * table_width, "blue", attrs=["bold"])
            cprint(msg, "blue", attrs=["bold"])
            cprint("=" * table_width, "blue", attrs=["bold"])
            print
            print output

    def do_user(self, args):
        """user:\n  set user"""
        username = args.split()[0]
        self.user = username

    def do_ls(self, args):
        """ls:\n  list directory (using shell cmd)"""
        return self.__os_cmd("ls", args)

    def do_cd(self, args):
        """cd:\n  change working directory"""
        if not args:
            newdir = os.getenv('HOME')
        else:
            newdir = args.split()[0]

        try:
            os.chdir(newdir)
        except OSError as e:
            error("Can't change dir to %s: %s" % (newdir, e.message))

    def complete_cd(self, text, line, begidx, endidx):
        return self.__file_completion(text)

    def do_pwd(self, args):
        """pwd:\n  print current working directory"""
        print os.getcwd()

    def __os_cmd(self, cmd, args):
        args = [cmd] + args.split()
        p = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
        p.wait()
        for line in p.stderr:
            sys.stderr.write(colored(line, 'red'))
        for line in p.stdout:
            sys.stdout.write(line)

    def __file_completion(self, text):
        srcdir = os.path.dirname(text)
        ftext = os.path.basename(text)

        listing = os.listdir(srcdir) if srcdir else os.listdir('.')

        if srcdir and not srcdir.endswith('/'):
            srcdir = srcdir + '/'

        listing = [srcdir + x for x in listing if x.startswith(ftext)]
        full = []

        for item in listing:
            if os.path.isdir(item):
                full += [item + '/' + x for x in os.listdir(item)]
            else:
                full.append(item)

        full.sort()
        return full

    def run_alias(self, args):
        import functools


    def do_alias(self, args):
        alias_name, script = args.split(None, 1)
        func_name = "do_" + alias_name
        if hasattr(self, func_name):
            if getattr(self, func_name) != self.run_alias:
                error("Can't overwrite built-in command")
                return
        setattr(self, func_name, self.run_alias)
        self.alias_scripts[alias_name] = script
        export_print("Alias %s has been created" % alias_name)