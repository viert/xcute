from pyparsing import Word, srange, ZeroOrMore, \
    OneOrMore, Suppress, Optional, Literal, Group, stringEnd
from collections import namedtuple

class ConductorExpression(object):

    CommonName = Word(srange("[a-zA-Z0-9_\-\.]"))
    SingleDomainName = Word(srange("[0-9a-z_\-]"))

    InclusionOperator = Optional(Literal("-") | Literal("+"), default="+")("inclusion").setParseAction(lambda x: x[0])

    HostName = Group(SingleDomainName +
                     OneOrMore("." + SingleDomainName)
                     )("host").setParseAction(lambda x: ''.join(x[0]))

    GroupName = Group(Suppress("%") + CommonName)("group").setParseAction(lambda x: ''.join(x[0]))
    ProjectName = Group(Suppress("*") + CommonName)("project").setParseAction(lambda x: ''.join(x[0]))
    EntireHostList = Literal("*")("entire").setParseAction(lambda x: x[0])

    DatacenterFilter = (Suppress("@") + CommonName)("datacenter").setParseAction(lambda x: x[0])

    Tag = (Suppress("#") + CommonName).setParseAction(lambda x: x[0])
    FieldKey = CommonName("key")
    FieldValue = Word(srange("[a-zA-Z0-9_\-\.:]"))("value")
    FieldDescription = (Suppress("[") +
                        FieldKey +
                        Suppress("=") +
                        FieldValue +
                        Suppress("]")).setParseAction(lambda k: {k[0]: k[1]})
    HostListToken = Group(InclusionOperator + (GroupName | ProjectName | HostName | EntireHostList))\
        ("token")
    Filters = ZeroOrMore(Tag | FieldDescription)("filters")

    Expression = (HostListToken +
                           Optional(DatacenterFilter) +
                           Filters + stringEnd)("expression")

    Expression.setWhitespaceChars("")

    __slots__ = (
        "token",
        "datacenter_filter",
        "tags_filter",
        "fields_filter",
        "result",
        "exclude"
    )

    ListToken = namedtuple("ListToken", field_names=["type", "data"])

    def __init__(self, token):
        self.datacenter_filter = None
        self.tags_filter = None
        self.fields_filter = None
        result = self.Expression.parseString(token)
        self.result = result

        if result.token.host != "":
            self.token = ConductorExpression.ListToken("host", result.token.host)
        elif result.token.group != "":
            self.token = ConductorExpression.ListToken("group", result.token.group)
        elif result.token.project != "":
            self.token = ConductorExpression.ListToken("project", result.token.project)
        elif result.token.entire != "":
            self.token = ConductorExpression.ListToken("entire", "")

        if result.token.inclusion == "-":
            self.exclude = True
        else:
            self.exclude = False

        if result.datacenter != "":
            self.datacenter_filter = result.datacenter


        if result.filters != "":
            filter_list = result.filters.asList()
            for f in filter_list:
                if type(f) == str:
                    if self.tags_filter is None:
                        self.tags_filter = []
                    self.tags_filter.append(f)
                else:
                    if self.fields_filter is None:
                        self.fields_filter = {}
                    self.fields_filter.update(f)

    def __str__(self):
        return "[ConductorExpression token=<type:%s, data:%s> exclude=%s datacenter_filter=<%s> " \
               "tags_filter=<%s> fields_filter=<%s>]" % ( self.token.type,
                                                          self.token.data,
                                                          self.exclude,
                                                          self.datacenter_filter,
                                                          self.tags_filter,
                                                          self.fields_filter )


if __name__ == '__main__':
    expressions = [
        "-host1.example.com",
        "%corba",
        "%corba@iva",
        "*market",
        "*#tag1#tag2",
        "-%infra@sgdc[role=kubernetes::master]#tag",
        "*[_role=kuber-id]"
    ]

    for e in expressions:
        print e
        print ConductorExpression(e)