import json
import os
import time
import requests
import cPickle as pickle
from collections import defaultdict
from xclib.conductor.models import Datacenter, Project, Group, Host
from xclib.conductor.parser import ConductorExpression


class CacheExpired(Exception):
    pass


class Autocompleter(object):

    MAX_LENGTH = 10

    def __init__(self, max_length=MAX_LENGTH):
        self.mxl = max_length
        self.data = defaultdict(set)

    def add(self, item):
        keylength = self.mxl if self.mxl < len(item) else len(item)
        key = item[:keylength]

        for i in xrange(keylength):
            self.data[key[:i + 1]].add(item)

    def remove(self, item):
        key_length = self.mxl if self.mxl < len(item) else len(item)
        key = item[:key_length]

        for i in xrange(key_length):
            self.data[key[:i + 1]].remove(item)

    def complete(self, key):
        if not key:
            keys = [x for x in self.data.keys() if len(x) == 1]
            data = set([])
            for key in keys:
                data = data.union(self.data[key])
            return data

        if len(key) <= self.mxl:
            return self.data[key]
        else:
            prekey = key[:self.mxl]
            return set([x for x in self.data[prekey] if x.startswith(key)])


class ConductorError(Exception):
    def __init__(self, status, body):
        Exception.__init__(self)
        self.status = status
        self.body = body

    def __repr__(self):
        return "ConductorError status_code=%d, body follows:\n%s" % (self.status, self.body)

    def __str__(self):
        return "ConductorError status_code=%d, body follows:\n%s" % (self.status, self.body)


class Api(object):

    CLASS_KEY = None

    def __init__(self, conductor):
        self.__c = conductor

    def get(self, key, value):
        cache = self.__c.cache[self.CLASS_KEY][key]
        if type(cache) == defaultdict:
            return cache[value]
        else:
            return cache.get(value)

    def get_cache(self, key):
        return self.__c.cache[self.CLASS_KEY][key]


class ProjectApi(Api):
    CLASS_KEY = Project

    def all(self):
        cache = self.get_cache(Project.KEY)
        return set(cache.values())


class DatacenterApi(Api):
    CLASS_KEY = Datacenter


class HostApi(Api):
    CLASS_KEY = Host


class GroupApi(Api):
    CLASS_KEY = Group


class Conductor(object):
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = 5000
    DEFAULT_CACHE_DIR = os.path.join(os.getenv("HOME"), ".xcute_cache")
    DEFAULT_CACHE_TTL = 3600

    def __init__(self, projects,
                 cache_ttl=DEFAULT_CACHE_TTL,
                 host=DEFAULT_HOST,
                 port=DEFAULT_PORT,
                 cache_dir=DEFAULT_CACHE_DIR,
                 drop_cache=False,
                 print_func=None):
        self.print_func = print_func
        self.cache_ttl = cache_ttl
        self.cache_dir = cache_dir
        self.project_list = projects
        self.datacenters = DatacenterApi(self)
        self.projects = ProjectApi(self)
        self.groups = GroupApi(self)
        self.hosts = HostApi(self)
        self.cache = None
        self.autocompleters = None

        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)

        project_list = ",".join(projects)
        self.ex_url = "http://%s:%d/api/v1/open/executer_data?projects=%s&recursive=true" % (host, port, project_list)

        if drop_cache:
            self.fetch()
        else:
            try:
                self.load()
            except:
                if self.print_func:
                    self.print_func("Reloading data from conductor...")
                self.fetch()

    def reset_cache(self):
        self.cache = {
            Datacenter: {
                "_id": {},
                Datacenter.KEY: {}
            },
            Group: {
                "_id": {},
                "project_id": defaultdict(set),
                Group.KEY: {}
            },
            Host: {
                "_id": {},
                "group_id": defaultdict(set),
                Host.KEY: {}
            },
            Project: {
                "_id": {},
                Project.KEY: {}
            }
        }
        self.autocompleters = {
            Datacenter: Autocompleter(),
            Group: Autocompleter(),
            Host: Autocompleter(),
            Project: Autocompleter(),
            "tags": Autocompleter(),
            "field_keys": Autocompleter(),
            "field_values": Autocompleter()
        }

    @property
    def cache_filename(self):
        filename = "cache_" + ".".join(self.project_list) + Host.STORE_VERSION + ".pickle"
        return os.path.join(self.cache_dir, filename)

    @property
    def autocompleters_filename(self):
        filename = "ac_" + ".".join(self.project_list) + Host.STORE_VERSION + ".pickle"
        return os.path.join(self.cache_dir, filename)

    def load(self, fallback=False):
        with open(self.cache_filename) as cf:
            data = pickle.load(cf)
            if time.time() - data["ts"] > self.cache_ttl and not fallback:
                raise CacheExpired()
            else:
                self.cache = data["data"]
        with open(self.autocompleters_filename) as cf:
            data = pickle.load(cf)
            if time.time() - data["ts"] > self.cache_ttl and not fallback:
                raise CacheExpired()
            else:
                self.autocompleters = data["data"]

    def save(self):
        if self.print_func:
            self.print_func("Saving cache...")
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)
        with open(self.cache_filename, "w") as cf:
            pickle.dump({ "ts": time.time(), "data": self.cache }, cf)
        with open(self.autocompleters_filename, "w") as cf:
            pickle.dump({ "ts": time.time(), "data": self.autocompleters }, cf)

    def fetch(self):
        self.reset_cache()
        try:
            response = requests.get(self.ex_url)
            if response.status_code != 200:
                raise ConductorError(response.status_code, response.content)
        except Exception as e:
            if self.print_func:
                self.print_func(e)
                self.print_func("Error getting data from conductor, falling back to cache")
            try:
                self.load(fallback=True)
            except IOError:
                # Here we don't have any cache and don't have a conductor connection
                self.print_func("No conductor connection and no cache found, running xcute for the first time? Please configure xcute properly editing ~/.xcute.conf file. An example has been already there.")
                return
            return
        data = json.loads(response.content)["data"]

        for dc_params in data["datacenters"]:
            datacenter = Datacenter(self, **dc_params)
            self.cache[Datacenter]["_id"][datacenter._id] = datacenter
            self.cache[Datacenter][Datacenter.KEY][dc_params[Datacenter.KEY]] = datacenter
            self.autocompleters[Datacenter].add(dc_params[Datacenter.KEY])

        for p_params in data["projects"]:
            project = Project(self, **p_params)
            self.cache[Project]["_id"][project._id] = project
            self.cache[Project][Project.KEY][p_params[Project.KEY]] = project
            self.autocompleters[Project].add(p_params[Project.KEY])

        for g_params in data["groups"]:
            group = Group(self, **g_params)
            self.cache[Group]["_id"][group._id] = group
            self.cache[Group][Group.KEY][g_params[Group.KEY]] = group
            self.cache[Group]["project_id"][group.project_id].add(group)
            self.autocompleters[Group].add(g_params[Group.KEY])

        for h_params in data["hosts"]:
            host = Host(self, **h_params)
            self.cache[Host]["_id"][host._id] = host
            self.cache[Host][Host.KEY][h_params[Host.KEY]] = host
            self.autocompleters[Host].add(h_params[Host.KEY])
            if "all_tags" in h_params and len(h_params["all_tags"]) > 0:
                for tag in h_params["all_tags"]:
                    self.autocompleters["tags"].add(tag)
            if "all_custom_fields" in h_params and len(h_params["all_custom_fields"]) > 0:
                for field in h_params["all_custom_fields"]:
                    self.autocompleters["field_keys"].add(field["key"]+"=")
                    self.autocompleters["field_values"].add(field["value"])

            if host.group_id is not None:
                self.cache[Host]["group_id"][host.group_id].add(host)

        self.save()

    def resolve(self, expr):
        tokens = expr.split(",")
        result_hosts = set()

        for token in tokens:
            parsed = ConductorExpression(token)
            hosts = set()
            rawhost = None

            if parsed.token.type == "group":
                group = self.groups.get(Group.KEY, parsed.token.data)
                if group is not None:
                    hosts = set(group.all_hosts)
            elif parsed.token.type == "project":
                project = self.projects.get(Project.KEY, parsed.token.data)
                if project:
                    for group in project.groups:
                        hosts = hosts.union(group.all_hosts)
            elif parsed.token.type == "entire":
                projects = self.projects.all()
                for project in projects:
                    for group in project.groups:
                        hosts = hosts.union(group.all_hosts)
            else:
                host = self.hosts.get(Host.KEY, parsed.token.data)
                if host:
                    hosts.add(host)
                else:
                    rawhost = parsed.token.data

            if len(hosts) > 0:
                if parsed.datacenter_filter:
                    hosts = set([h for h in hosts if h.in_datacenter(parsed.datacenter_filter)])
                if parsed.tags_filter:
                    hosts = set([h for h in hosts
                                 if set(h.all_tags).intersection(parsed.tags_filter) == set(parsed.tags_filter)])
                if parsed.fields_filter:
                    for key, value in parsed.fields_filter.items():
                        field_data = { "key": key, "value": value }
                        hosts = set([h for h in hosts if field_data in h.all_custom_fields])
                if parsed.exclude:
                    result_hosts = result_hosts.difference(set([x.fqdn for x in hosts]))
                else:
                    result_hosts = result_hosts.union(set([x.fqdn for x in hosts]))

            if rawhost:
                # raw hosts are always filtered out if any filters exist
                if parsed.datacenter_filter is None and \
                                parsed.fields_filter is None and \
                                parsed.tags_filter is None:
                    if parsed.exclude:
                        try:
                            result_hosts.remove(rawhost)
                        except:
                            pass
                    else:
                        result_hosts.add(rawhost)

        return result_hosts
