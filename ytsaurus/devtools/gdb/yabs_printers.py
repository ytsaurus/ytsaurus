# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division

import gdb

import re
import socket


def concat(func):
    def wrapper(*args, **kws):
        return ''.join(func(*args, **kws))

    return wrapper


def identity(x):
    return x


def coerce(value, to_, via=None):
    if isinstance(value, to_):
        return value
    elif via is not None:
        return via(value)
    return to_(value)


def as_gdb_value(x):
    return coerce(x, to_=gdb.Value, via=gdb.parse_and_eval)


def as_gdb_type(x):
    return coerce(x, to_=gdb.Type, via=gdb.lookup_type)


def plural(count):
    if count == 1:
        return ''
    else:
        return 's'


def function_name(value):
    m = re.search(r'<([^(]+)\(.*>', str(value))
    if m:
        return m.group(1)


@concat
def repr_list(name, items):
    yield '{name} {{'.format(name=name)
    for i, item in enumerate(items):
        if i:
            yield ',\n'
        else:
            yield '\n'
        for j, line in enumerate(str(item).splitlines()):
            if j:
                yield '\n'
            yield '  ' + line
    yield '\n}'


@concat
def repr_hash(name, hash):
    yield '{name} (size={size}, count={count}) {{'.format(
        name=name,
        size=hash.size,
        count=hash.count,
    )
    for bucket_index, bucket in enumerate(hash):
        if bucket is None:
            continue
        yield '\n'
        yield '  %{index} {{'.format(index=bucket_index)
        for i, item in enumerate(bucket):
            if i:
                yield ',\n'
            else:
                yield '\n'
            for j, line in enumerate(str(item.cast(hash.key_type)).splitlines()):
                if j:
                    yield '\n'
                yield '    ' + line
            yield ' => '
            for j, line in enumerate(str(item).splitlines()):
                if j:
                    yield '\n'
                    yield '    ' + line
                else:
                    yield line
        yield '\n  }'
    yield '\n}'


@concat
def repr_tree(name, tree):
    yield '{name} {{'.format(name=name)
    for i, item in enumerate(tree.nodes):
        if i:
            yield ',\n'
        else:
            yield '\n'
        for j, line in enumerate(str(item.key).splitlines()):
            if j:
                yield '\n'
            yield '  ' + line
        yield ' => '
        for j, line in enumerate(str(item.value).splitlines()):
            if j:
                yield '\n'
                yield '  ' + line
            else:
                yield line
    yield '\n}'


def get_pair_element(val, val_type):
    if val_type.template_argument(2):
        return val.cast(val_type.template_argument(0))
    else:
        return val['__value_']


def destructure_compressed_pair(val):
    pair_type = val.type
    base_types = pair_type.fields()
    base_type_first = base_types[0].type
    base_type_second = base_types[1].type
    return (get_pair_element(val.cast(base_type_first), base_type_first),
            get_pair_element(val.cast(base_type_second), base_type_second))


###############################################################################
#                                    List                                     #
###############################################################################

class OldList(object):
    """Inspect yabs_list contents"""

    def __init__(self, value, elem_type):
        self.value = as_gdb_value(value)
        # assert self.value.type == gdb.lookup_type('yabs_list').pointer()

        self.elem_type = as_gdb_type(elem_type)
        self.elem_ptr_type = self.elem_type.pointer()

    def __iter__(self):
        return self.elements

    def __repr__(self):
        return repr_list('yabs_list', self.elements)

    @property
    def nodes(self):
        node = self.value
        while node:
            yield node
            node = node.dereference()['next']

    @property
    def elements(self):
        return (self.data(_) for _ in self.nodes)

    def data(self, node):
        return (node + 1).cast(self.elem_ptr_type).dereference()


class List(object):
    """Inspect yabs::list_t<...> contents"""

    def __init__(self, value):
        self.value = as_gdb_value(value)
        self.elem_type = self.value.type.template_argument(0)
        self.elem_ptr_type = self.elem_type.pointer()

    @property
    def nodes(self):
        node = self.value['first']
        while node:
            yield node
            node = node.dereference()['next']

    @property
    def elements(self):
        return (self.data(_) for _ in self.nodes)

    def data(self, node):
        return (node + 1).cast(self.elem_ptr_type).dereference()

    def __repr__(self):
        return repr_list(
            self.value.type,
            self.elements
        )

    def __iter__(self):
        return self.elements


###############################################################################
#                                    Hash                                     #
###############################################################################

class OldHash(object):
    """Inspect yabs_hash contents"""

    def __init__(self, value, value_type, key_type=None):
        if key_type is None:
            key_type = value_type

        self.value = as_gdb_value(value)
        # assert str(self.value.type.strip_typedefs()) == 'yabs_hash'

        self.key_type = as_gdb_type(key_type)
        self.value_type = as_gdb_type(value_type)

    def __len__(self):
        return self.size

    def __getitem__(self, index):
        if not 0 <= index < self.size:
            raise IndexError('Bad index')
        bucket = (self.value['hashtable'] + index).dereference()
        if bucket:
            return OldList(bucket, self.value_type)

    def __repr__(self):
        return repr_hash('yabs_hash', self)

    @property
    def size(self):
        return int(self.value['size'])

    @property
    def count(self):
        return int(self.value['count'])


class Hash(object):
    """Inspect yabs::hash_t<...> contents"""

    def __init__(self, value):
        self.value = as_gdb_value(value)

        self.value_type = self.value.type.template_argument(0)
        self.key_type = self.value.type.template_argument(1)

    def __len__(self):
        return self.size

    def __getitem__(self, index):
        if not 0 <= index < self.size:
            raise IndexError('Bad index')
        bucket = (self.value['hashtable'] + index).dereference()
        if bucket['first']:
            return List(bucket)

    def __repr__(self):
        return repr_hash(self.value.type, self)

    @property
    def size(self):
        return int(self.value['table_size'])

    @property
    def count(self):
        return int(self.value['count'])


class ExpHash(object):
    """Inspect yabs::exp::hash_t<...> contents"""

    def __init__(self, value):
        self.value = as_gdb_value(value)
        self.table = destructure_compressed_pair(self.value['table']['__ptr_'])[0].dereference()

        self.value_type = self.value.type.template_argument(0)
        self.key_type = self.value.type.template_argument(1)

    def __len__(self):
        return self.size

    def __getitem__(self, index):
        if not 0 <= index < self.size:
            raise IndexError('Bad index')
        bucket = self.table['buckets'][index]
        if bucket['first']:
            return List(bucket)

    def __repr__(self):
        return repr_hash(self.value.type, self)

    @property
    def size(self):
        return int(self.table['bucket_count'])

    @property
    def count(self):
        return int(self.table['elem_count'])


###############################################################################
#                                    Tree                                     #
###############################################################################

class TreeNode(object):
    def __init__(self, node_ptr, value_type, key_type):
        assert node_ptr

        self.node_ptr = node_ptr
        self.node = node_ptr.dereference()
        self.value_type = value_type
        self.key_type = key_type

    def _get_node(self, ptr):
        if not ptr:
            return None
        return type(self)(ptr, self.value_type, self.key_type)

    @property
    def has_right_child(self):
        return self.node['has_right_child']

    @property
    def left(self):
        return self._get_node(self.node['left'])

    @property
    def right(self):
        if self.has_right_child:
            return self._get_node(self.node['right'])

    @property
    def parent(self):
        if not self.has_right_child:
            return self._get_node(self.node['right'])

    @property
    def leftmost(self):
        prev = self
        node = self.left
        while node is not None:
            prev = node
            node = node.left
        return prev

    @property
    def key(self):
        return (self.node_ptr + 1) \
            .cast(self.key_type.pointer()) \
            .dereference()

    @property
    def value(self):
        return (self.node_ptr + 1) \
            .cast(self.value_type.pointer()) \
            .dereference()

    def iter_tree(self):
        node = self.leftmost
        while node:
            yield node
            right = node.right
            if right:
                node = right.leftmost
            else:
                node = node.parent


class Tree(object):
    """Inspect yabs::tree_t<...> contents"""

    def __init__(self, value):
        self.value = as_gdb_value(value)

        self.value_type = self.value.type.template_argument(0)
        self.key_type = self.value.type.template_argument(1)

    @property
    def root(self):
        root = self.value['root']
        if root:
            return TreeNode(root, self.value_type, self.key_type)

    @property
    def nodes(self):
        if self.root is None:
            return

        for node in self.root.iter_tree():
            yield node

    def __repr__(self):
        return repr_tree(self.value.type, self)


class OldTreeNode(TreeNode):
    @property
    def has_right_child(self):
        return self.node['rtag'] == 0


class OldTree(object):
    """Inspect (yabs_tree *) contents"""

    def __init__(self, value, value_type, key_type=None):
        if key_type is None:
            key_type = value_type

        self.value = as_gdb_value(value)
        # assert self.value.type == gdb.lookup_type('yabs_tree').pointer()

        self.value_type = as_gdb_type(value_type)
        self.key_type = as_gdb_type(key_type)

    @property
    def root(self):
        if self.value:
            return OldTreeNode(self.value, self.value_type, self.key_type)

    @property
    def nodes(self):
        if self.root is None:
            return

        for node in self.root.iter_tree():
            yield node

    def __repr__(self):
        return repr_tree(self.value.type, self)


###############################################################################
#                                    IpV6                                     #
###############################################################################

class IpV6(object):
    """Inspect ipv6 addresses represented by _uint128_t"""

    bytes_type = gdb.lookup_type('unsigned char').array(16)

    def __init__(self, value):
        self.value = as_gdb_value(value)
        assert self.value.type.sizeof == 16

        bytes_value = self.value.cast(self.bytes_type)
        self.addr = ''.join(
            chr(bytes_value[_])
            for _ in reversed(range(16))
        )

    def __str__(self):
        return socket.inet_ntop(socket.AF_INET6, self.addr)


###############################################################################
#                                  Printers                                   #
###############################################################################

class OldListPtrPrinter(object):
    """Print (yabs_list *) properties"""

    def __init__(self, value):
        self.list = OldList(value, 'void')

    def to_string(self):
        length = sum(1 for _ in self.list.nodes)
        if length == 0:
            return '{type} = (null)'.format(
                type=self.list.value.type,
            )
        return '{type} = ({length} node{s})'.format(
            type=self.list.value.type,
            length=length,
            s=plural(length)
        )


class OldHashPrinter(object):
    """Print yabs_hash properties"""

    def __init__(self, value):
        self.hash = OldHash(value, 'void')

    def to_string(self):
        return '{type} = ({size} bucket{sb}, {count} value{sv}, hash: {fhash}, cmp: {fcmp})'.format(
            type=self.hash.value.type,
            size=self.hash.size,
            sb=plural(self.hash.size),
            count=self.hash.count,
            sv=plural(self.hash.count),
            fhash=function_name(self.hash.value['hash']),
            fcmp=function_name(self.hash.value['compar']),
        )


class OldTreePtrPrinter(object):
    """Print (yabs_tree *) properties"""

    def __init__(self, value):
        self.tree = OldTree(value, 'void')

    def to_string(self):
        root = self.tree.root
        if root is None:
            return '{type} = (null)'.format(
                type=self.tree.value.type,
            )
        length = sum(1 for _ in self.tree.nodes)
        return '{type} = ({length} node{s})'.format(
            type=self.tree.value.type,
            length=length,
            s=plural(length),
        )


class ListPrinter(object):
    """Print yabs::list_t contents"""

    def __init__(self, value):
        self.value = value
        self.list = List(value)

    def to_string(self):
        return str(self.value.type)

    def display_hint(self):
        return 'array'

    def children(self):
        for n, elem in enumerate(self.list.elements):
            yield '[{}]'.format(n), elem


class SlListPrinter(object):
    """Print yabs::sl_list_t<...> contents"""

    def __init__(self, value):
        self.value = value
        self.list = List(value)

    def to_string(self):
        return '{type} of {count} item{s}'.format(
            type=self.value.type,
            count=self.value['count'],
            s=plural(self.value['count']),
        )

    def display_hint(self):
        return 'array'

    def children(self):
        for n, elem in enumerate(self.list.elements):
            yield '[{}]'.format(n), elem


class HashPrinter(object):
    """Print yabs::hash_t<...> contents"""

    def __init__(self, value):
        self.value = value
        try:
            value['hashtable']
            self.hash = Hash(value)
        except gdb.error:
            self.hash = ExpHash(value)

    def to_string(self):
        return '{type} of {count} item{s}'.format(
            type=self.value.type,
            count=self.hash.count,
            s=plural(self.hash.count),
        )

    def display_hint(self):
        return 'map'

    def children(self):
        for i, bucket in enumerate(self.hash):
            if bucket is None:
                continue
            for j, value in enumerate(bucket):
                yield '[{}, {}].key'.format(i, j), value.cast(self.hash.key_type)
                yield '[{}, {}].value'.format(i, j), value


class TreePrinter(object):
    """Print yabs::tree_t<...> contents"""

    def __init__(self, value):
        self.value = value
        self.tree = Tree(value)

    def to_string(self):
        return '{type}'.format(
            type=self.value.type,
        )

    def display_hint(self):
        return 'map'

    def children(self):
        for n, node in enumerate(self.tree.nodes):
            yield '[{}].key'.format(n), node.key
            yield '[{}].value'.format(n), node.value


class AvlTreePrinter(TreePrinter):
    """Print yabs::avl_tree_t<...> contents"""

    def to_string(self):
        return '{type} of {count} item{s}'.format(
            type=self.value.type,
            count=self.value['count'],
            s=plural(self.value['count'])
        )


class ValuePrinter(object):
    """Print yabs_value contents"""

    TYPE_STR = 1 << 0
    TYPE_TID = 1 << 1
    TYPE_I32 = 1 << 2
    TYPE_U32 = 1 << 3
    TYPE_U64 = 1 << 4
    TYPE_GC = 1 << 5

    def __init__(self, value):
        self.value = value

    def to_string(self):
        return 'yabs_value'

    def children(self):
        type_ = self.value['type']
        if type_ & self.TYPE_STR:
            yield 'str', self.value['s']['text']
        elif type_ & self.TYPE_I32:
            yield 'i32', self.value['d']['i32']
        elif type_ & self.TYPE_U32:
            yield 'u32', self.value['d']['u32']
        elif type_ & self.TYPE_U64:
            yield 'u64', self.value['d']['u64']
        elif type_ & self.TYPE_TID:
            yield 'tid', self.value['s']['id']
            yield 'text', self.value['s']['text']
        elif type_ & self.TYPE_GC:
            yield 'base', self.value['gc']['base']
            yield (
                'user_goal_hash',
                self.value['gc']['user_goal_hash'].dereference()
            )


class ArgPrinter(object):
    """Print yabs_arg contents"""

    FIELDS = [
        'data',
        'keyword_id',
        'flags',
        'arg_tag',
        'tag',
        'search_freq',
        'update_time',
        'weight',
        'serial_no',
    ]

    def __init__(self, value):
        self.value = value

    def to_string(self):
        return 'yabs_arg'

    def children(self):
        for field in self.FIELDS:
            yield field, self.value[field]


class UInt128Printer(object):
    """Print IPv6 addresses stored as LE 128-bit integers"""

    def __init__(self, value):
        self.value = value
        self.ipv6 = IpV6(value)

    def to_string(self):
        # TODO: show uint value too
        return 'ipv6({ipv6})'.format(
            ipv6=self.ipv6,
        )


class TStringPrinter(object):
    """Print TString contents without crashing GDB"""

    def __init__(self, value):
        self.value = value

    def to_string(self):
        expr = '(*(TString *)({addr})).c_str()'.format(
            addr=self.value.address,
        )
        return 'TString = {}'.format(
            gdb.parse_and_eval(expr)
        )


###############################################################################
#                                  Functions                                  #
###############################################################################

class IpV6Function(gdb.Function):
    def __init__(self):
        super(IpV6Function, self).__init__('ipv6')

    def invoke(self, value):
        return str(IpV6(value))


###############################################################################
#                                 Bookkeeping                                 #
###############################################################################

class RegexpCollectionPrettyPrinter(gdb.printing.RegexpCollectionPrettyPrinter):
    def add_ptr_printer(self, name, cls):
        super(RegexpCollectionPrettyPrinter, self).add_printer(
            name,
            r'^' + name + r' *\*$',
            cls
        )

    def add_printer(self, name, cls):
        super(RegexpCollectionPrettyPrinter, self).add_printer(
            name,
            r'^' + name + r'$',
            cls
        )

    def add_template_printer(self, name, cls):
        super(RegexpCollectionPrettyPrinter, self).add_printer(
            name,
            r'^' + name + r'<.*>$',
            cls
        )

    def __call__(self, value):
        value_type = value.type
        nptr = 0
        while value_type.code == gdb.TYPE_CODE_PTR:
            value_type = value_type.target()
            nptr += 1

        typename = gdb.types.get_basic_type(value_type).tag
        if not typename:
            typename = value_type.name
        if not typename:
            return None

        typename += '*' * nptr
        for printer in self.subprinters:
            if printer.enabled and printer.compiled_re.search(typename):
                return printer.gen_printer(value)


def register_pretty_printers():
    p = RegexpCollectionPrettyPrinter('yabs')

    p.add_ptr_printer('yabs_list', OldListPtrPrinter)
    p.add_ptr_printer('yabs_tree', OldTreePtrPrinter)
    p.add_printer('yabs_hash', OldHashPrinter)

    p.add_ptr_printer('yabs_load_list', OldListPtrPrinter)
    p.add_ptr_printer('yabs_load_tree', OldTreePtrPrinter)
    p.add_printer('yabs_load_hash', OldHashPrinter)

    p.add_printer('yabs_value', ValuePrinter)
    p.add_printer('yabs_arg', ArgPrinter)
    p.add_printer('_uint128_t', UInt128Printer)
    p.add_printer('TString', TStringPrinter)

    p.add_template_printer('yabs::list_t', ListPrinter)
    p.add_template_printer('yabs::sl_list_t', SlListPrinter)
    p.add_template_printer('yabs::hash_t', HashPrinter)
    p.add_template_printer('yabs::exp::hash_t', HashPrinter)
    p.add_template_printer('yabs::tree_t', TreePrinter)
    p.add_template_printer('yabs::avl_tree_t', AvlTreePrinter)

    gdb.printing.register_pretty_printer(None, p, replace=True)


def register_functions():
    IpV6Function()


class YabsEnable(gdb.Command):
    """Enable YaBS functions and printers"""

    def __init__(self):
        super(YabsEnable, self).__init__("yabs-enable", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        register_functions()
        register_pretty_printers()
        print("[yabs] YaBS GDB pretty-printers enabled")
