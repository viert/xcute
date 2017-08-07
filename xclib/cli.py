from __future__ import print_function
from gevent import Greenlet
from gevent.select import select
from gevent.pool import Pool
from gevent.subprocess import Popen, PIPE
from collections import defaultdict
from termcolor import colored as term_colored
from xclib.conductor import Conductor
from xclib.conductor.models import Datacenter, Project, Host, Group
import sys, fcntl, termios, struct, os, cmd, re


try:
    import gnureadline as readline
except ImportError:
    import readline

sys.modules["readline"] = readline
readline.parse_and_bind("tab: complete")


def terminal_size():
    h, w, hp, wp = struct.unpack('HHHH',
        fcntl.ioctl(0, termios.TIOCGWINSZ,
        struct.pack('HHHH', 0, 0, 0, 0)))
    return w, h


def colored(*args, **kwargs):
    if "sym_ignore" in kwargs:
        sym_ignore = kwargs["sym_ignore"]
        del(kwargs["sym_ignore"])
    else:
        sym_ignore = False
    result = term_colored(*args, **kwargs)
    if not sym_ignore:
        return result
    return re.sub(r'(\x1b\[\d+m)', '\x01\g<1>\x02', result)


def cprint(*args, **kwargs):
    print colored(*args, **kwargs)


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
        "progressgbar": True,
        "ping_count": 5
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
        self.ping_count = options.get("ping_count") or self.DEFAULT_OPTIONS["ping_count"]
        self.finished = False
        self.alias_scripts = {}
        self.default_remote_dir = options.get("default_remote_dir") or "/tmp"
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
            mode = colored("[Collapse]", "green", sym_ignore=True)
        elif self.mode == "parallel":
            mode = colored("[Parallel]", "yellow", sym_ignore=True)
        elif self.mode == "serial":
            mode = colored("[Serial]", "cyan", sym_ignore=True)

        return "%s %s> " % (mode, colored(self.user, "blue", attrs=["bold"], sym_ignore=True))

    def emptyline(self):
        pass

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
          warn("Can't read history file")

    def postloop(self):
        try:
            readline.write_history_file(self.HISTORY_FILE)
        except (OSError, IOError) as e:
            warn("Can't write history file: %s" % e.message)

    def do_shell(self, s):
        os.system(s)

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

    def do_describe(self, args):
        """host:\n  shows conductor host data"""
        args = args.split()
        if len(args) <= 0:
            error("Usage: host <hostname>")
            return

        expr = args[0]
        hosts = self.conductor.resolve(expr)
        if len(hosts) != 1:
            error("You should provide exactly one host")
            return
        hostname = list(hosts)[0]
        host = self.conductor.hosts.get("fqdn", hostname)
        if host is None:
            error("Host %s not found" % hostname)
            return

        groups = set()
        groups.add(host.group)
        groups = groups.union(host.group.all_parents)
        groups = [group.name for group in groups]
        groups.sort()

        tags = list(host.all_tags)
        tags.sort()

        hr = colored("="*50, "green")
        print(hr)
        print('  {host} {hostname}'.format(host=colored("Host:", "green"), hostname=hostname))
        print(hr + "\n")
        print('  {project} {project_name}\n'.format(project=colored("Project:", "green"),
                                                    project_name=host.group.project.name))
        print('  {}'.format(colored("Groups:", "green")))
        for group in groups:
            print('    {group}'.format(group=group))
        print("")
        print('  {}'.format(colored("Tags:", "green")))
        for tag in tags:
            print('    {tag}'.format(tag=tag))
        print("")


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

    def complete_describe(self, text, line, begidx, endidx):
        return self.complete_exec(text, line, begidx, endidx)

    def complete_ping(self, text, line, begidx, endidx):
        return self.complete_exec(text, line, begidx, endidx)

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

    @staticmethod
    def print_exec_results(codes):
        msg = " Hosts processed: %d, success: %d, error: %d    " % (codes["total"], codes["success"], codes["error"])
        hr = "=" * len(msg)
        cprint(hr, "green")
        cprint(msg, "green")
        cprint(hr, "green")

    def get_parallel_ssh_options(self, host, cmd):
        return [
            "ssh",
            "-l",
            self.user,
            "-o",
            "PubkeyAuthentication=yes",
            "-o",
            "PasswordAuthentication=no",
            host,
            cmd
        ]

    def run_parallel(self, hosts, cmd):
        codes = {"total": 0, "error": 0, "success": 0}

        def worker(host, cmd):
            p = Popen(self.get_parallel_ssh_options(host, cmd), stdout=PIPE, stderr=PIPE)
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
            p = Popen(self.get_parallel_ssh_options(host, cmd), stdout=PIPE, stderr=PIPE)
            o = ""
            while True:
                outs, _, _ = select([p.stdout, p.stderr], [], [])
                outline = errline = ""
                if p.stdout in outs:
                    outline = p.stdout.readline()
                if p.stderr in outs:
                    errline = p.stderr.readline()
                o += outline + errline

                if outline == "" and errline == "" and p.poll() is not None:
                    break

            if o == "":
                o = colored("[ No Output ]\n", "yellow")
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
            msg = " %s    " % ','.join(hosts)
            table_width = min([len(msg) + 2, terminal_size()[0]])
            cprint("=" * table_width, "blue", attrs=["bold"])
            cprint(msg, "blue", attrs=["bold"])
            cprint("=" * table_width, "blue", attrs=["bold"])
            print output

    def do_user(self, args):
        """user:\n  set user"""
        username = args.split()[0]
        self.user = username

    def do_ls(self, args):
        """ls:\n  list directory (using shell cmd)"""
        return self.__os_cmd("ls", args)

    def ping_parallel(self, hosts, pc):
        """ping:\n pings host (using shell cmd)"""
        codes = {"total": 0, "error": 0, "success": 0}
        def worker(host):
            if pc == 0:
                args = ["ping", host]
            else:
                args = ["ping", "-c", str(pc), host]
            p = Popen(args, stdout=PIPE, stderr=PIPE)
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
            pool.start(Greenlet(worker, host))
        pool.join()
        self.print_exec_results(codes)

    def do_ping(self, args):
        """ping:\n  pings hosts in parallel"""
        args = args.split()
        if len(args) == 0:
            error("Empty hostlist")
            return
        expr = args[0]
        if len(args) > 1:
            try:
                pc = int(args[1])
            except ValueError:
                error("Invalid ping count: should be integer")
                return
        else:
            pc = self.ping_count

        hosts = self.conductor.resolve(expr)
        if len(hosts) == 0:
            error("Empty hostlist")
            return
        self.ping_parallel(hosts, pc)

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

    def do_cat(self, args):
        return self.__os_cmd("cat", args)

    def complete_cat(self, text, line, begidx, endidx):
        return self.__file_completion(text)

    def do_distribute(self, args):
        """distribute:\n  copy local file to a group of servers into a specified directory"""
        args = args.split()
        if len(args) < 2:
            error("Usage: distribute <conductor_expression> <local_filename> [remote_dir=%s]" % self.default_remote_dir)
            return
        expr, filename = args[:2]
        hosts = self.conductor.resolve(expr)
        if len(hosts) == 0:
            error("Empty hostlist")
            return
        if not os.path.isfile(filename):
            error("%s is not a file or doesn't exist" % filename)
            return

        if len(args) > 2:
            remote_dir = args[2]
        else:
            remote_dir = self.default_remote_dir

        results = {
            "error": [],
            "success": [],
            "total": 0
        }
        errors = defaultdict(list)

        progress = None
        if self.progressbar:
            from progressbar import ProgressBar, Percentage, Bar, ETA, FileTransferSpeed
            progress = ProgressBar(
                widgets=["Running: ", Percentage(), ' ', Bar(marker='.'), ' ', ETA(), ' ', FileTransferSpeed()],
                maxval=len(hosts))

        def worker(host):
            p = Popen([
                "scp",
                "-B", # prevents asking for passwords
                filename,
                "%s@%s:%s" % (self.user, host, remote_dir)
            ], stdout=PIPE, stderr=PIPE)
            o, e = p.communicate()
            if p.poll() == 0:
                results["success"].append(host)
            else:
                results["error"].append(host)
                errors[e].append(host)

            results["total"] += 1
            if self.progressbar:
                progress.update(results["total"])

        if self.progressbar:
            progress.start()

        pool = Pool()
        for host in hosts:
            pool.start(Greenlet(worker, host))
        pool.join()

        if self.progressbar:
            progress.finish()

        if len(results["success"]) > 0:
            msg = "Successfully distributed to %d hosts" % len(results["success"])
            cprint(msg, "green")
        if len(results["error"]) > 0:
            cprint("There were errors distributing file", "red")
            for output, hosts in errors.items():
                msg = " %s    " % ','.join(hosts)
                table_width = min([len(msg) + 2, terminal_size()[0]])
                cprint("=" * table_width, "blue", attrs=["bold"])
                cprint(msg, "blue", attrs=["bold"])
                cprint("=" * table_width, "blue", attrs=["bold"])
                print output

    def complete_distribute(self, text, line, begidx, endidx):
        argnum = self.__completion_argnum(line, endidx)
        if argnum == 0:
            return self.complete_exec(text, line, begidx, endidx)
        elif argnum == 1:
            return self.__file_completion(text)
        else:
            return []

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

    def do_reload(self, args):
        export_print("Reloading data from conductor...")
        self.conductor.fetch()
