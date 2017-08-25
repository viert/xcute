
class ConductorObject(object):

    STORE_VERSION = "1.0.5"

    KEY = None
    FIELDS = []

    def __init__(self, **kwargs):
        self._id = None
        for key, value in kwargs.items():
            if key in self.FIELDS:
                setattr(self, key, value)

    def __repr__(self):
        return "<%s _id=\"%s\" %s=\"%s\">" % (self.__class__.__name__, self._id, self.KEY, getattr(self, self.KEY))


class Datacenter(ConductorObject):
    KEY = "name"

    FIELDS = [
        "_id",
        "name",
        "human_readable",
        "parent_id",
        "root_id",
        "created_at",
        "updated_at",
        "child_ids",
        "parent_id",
        "root_id"
    ]

    def __init__(self, conductor, **kwargs):
        ConductorObject.__init__(self, **kwargs)
        self.__c = conductor

    @property
    def children(self):
        return set([self.__c.datacenters.get("_id", ch_id) for ch_id in self.child_ids])

    @property
    def all_children(self):
        children = set(self.children)
        for ch in self.children:
            children = children.union(set(ch.all_children))
        return list(children)

    @property
    def parent(self):
        if self.parent_id is None:
            return None
        return self.__c.datacenters.get("_id", self.parent_id)

    @property
    def root(self):
        if self.root_id is None:
            return None
        return self.__c.datacenters.get("_id", self.root_id)


class Group(ConductorObject):
    KEY = "name"
    FIELDS = [
        "_id",
        "name",
        "description",
        "created_at",
        "updated_at",
        "project_id",
        "parent_ids",
        "child_ids",
    ]

    def __init__(self, conductor, **kwargs):
        ConductorObject.__init__(self, **kwargs)
        self.__c = conductor

    @property
    def children(self):
        return set([self.__c.groups.get("_id", ch_id) for ch_id in self.child_ids])

    @property
    def parents(self):
        return set([self.__c.groups.get("_id", p_id) for p_id in self.parent_ids])

    @property
    def project(self):
        return self.__c.projects.get("_id", self.project_id)

    @property
    def all_children(self):
        children = set(self.children)
        for ch in self.children:
            children = children.union(set(ch.all_children))
        return list(children)

    @property
    def all_parents(self):
        parents = set(self.parents)
        for p in self.parents:
            parents = parents.union(set(p.all_parents))
        return parents

    @property
    def hosts(self):
        return self.__c.hosts.get("group_id", self._id)

    @property
    def all_hosts(self):
        hosts = self.hosts
        for ch in self.children:
            hosts = hosts.union(set(ch.all_hosts))
        return hosts


class Host(ConductorObject):
    KEY = "fqdn"
    FIELDS = [
        "_id",
        "fqdn",
        "short_name",
        "group_id",
        "datacenter_id",
        "description",
        "all_tags",
        "all_custom_fields",
        "created_at",
        "updated_at",
    ]

    def __init__(self, conductor, **kwargs):
        ConductorObject.__init__(self, **kwargs)
        self.__c = conductor

    @property
    def group(self):
        return self.__c.groups.get("_id", self.group_id)

    @property
    def datacenter(self):
        return self.__c.datacenters.get("_id", self.datacenter_id)

    @property
    def root_datacenter(self):
        return self.datacenter.root

    def in_datacenter(self, dcname):
        if self.datacenter:
            if self.datacenter.name == dcname:
                return True
            else:
                pdc = self.datacenter.parent
                while pdc is not None:
                    if pdc.name == dcname:
                        return True
                    pdc = pdc.parent
        return False

class Project(ConductorObject):
    KEY = "name"

    FIELDS = [
        "_id",
        "name",
        "description",
        "email",
        "root_email",
        "owner_id",
        "updated_at",
        "created_at"
    ]

    def __init__(self, conductor, **kwargs):
        ConductorObject.__init__(self, **kwargs)
        self.__c = conductor

    @property
    def groups(self):
        return self.__c.groups.get("project_id", self._id)
