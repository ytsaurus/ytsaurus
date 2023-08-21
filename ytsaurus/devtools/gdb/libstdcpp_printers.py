"""\
GDB pretty-printers for libstdc++ and Arcadia-specific types

Copyright (C) 2008, 2009, 2010, 2011 Free Software Foundation, Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Forked and adopted for Arcadia by udovichenko-r@
"""


import os
import sys


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


import gdb
import itertools
import re
import unicodedata


_string_encoding = 'utf-8'

if sys.version_info[0] > 2:
    ### Python 3 stuff
    Iterator = object
    # Python 3 folds these into the normal functions.
    imap = map
    izip = zip
    # Also, int subsumes long
    long = int
else:
    ### Python 2 stuff
    class Iterator:
        """Compatibility mixin for iterators

        Instead of writing next() methods for iterators, write
        __next__() methods and use this mixin to make them work in
        Python 2 as well as Python 3.

        Idea stolen from the "six" documentation:
        <http://pythonhosted.org/six/#six.Iterator>
        """

        def next(self):
            return self.__next__()

    # In Python 2, we still need these from itertools
    from itertools import imap, izip

# Try to use the new-style pretty-printing if available.
_use_gdb_pp = True
try:
    import gdb.printing
except ImportError:
    _use_gdb_pp = False

# Try to install type-printers.
_use_type_printing = False
try:
    import gdb.types
    if hasattr(gdb.types, 'TypePrinter'):
        _use_type_printing = True
except ImportError:
    pass

_is_python2 = sys.version_info.major == 2

def print_type(typeobj):
    global _use_type_printing
    if not _use_type_printing:
        return str(typeobj)
    return gdb.types.apply_type_recognizers(
        gdb.types.get_type_recognizers(), typeobj) or str(typeobj)

class StdPointerPrinter:
    'Print a smart pointer of some kind'

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        if self.val['_M_refcount']['_M_pi'] == 0:
            return '%s (empty) %s' % (self.typename, self.val['_M_ptr'])
        return '%s (count %d) %s' % (
            self.typename,
            self.val['_M_refcount']['_M_pi']['_M_use_count'],
            self.val['_M_ptr'])


class UniquePointerPrinter:
    'Print a unique_ptr'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        return self.val['_M_t']


class StdListPrinter:
    'Print a std::list'

    class _iterator(Iterator):
        def __init__(self, nodetype, head):
            self.nodetype = nodetype
            self.base = head['_M_next']
            self.head = head.address
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.base == self.head:
                raise StopIteration
            elt = self.base.cast(self.nodetype).dereference()
            self.base = elt['_M_next']
            count = self.count
            self.count = self.count + 1
            return('[%d]' % count, elt['_M_data'])

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def children(self):
        itype = self.val.type.template_argument(0)
        # If the inferior program is compiled with -D_GLIBCXX_DEBUG
        # some of the internal implementation details change.
        if self.typename == 'std::list':
            nodetype = gdb.lookup_type('std::_List_node<%s>' % print_type(itype)).pointer()
        elif self.typename == 'std::__debug::list':
            nodetype = gdb.lookup_type(
                'std::__norm::_List_node<%s>' % print_type(itype)).pointer()
        else:
            raise ValueError('Cannot cast list node for list printer.')
        return self._iterator(nodetype, self.val['_M_impl']['_M_node'])

    def to_string(self):
        if self.val['_M_impl']['_M_node'].address == \
                self.val['_M_impl']['_M_node']['_M_next']:
            return 'empty %s' % (self.typename)
        return '%s' % (self.typename)


class StdListIteratorPrinter:
    'Print std::list::iterator'

    def __init__(self, typename, val):
        self.val = val
        self.typename = typename

    def to_string(self):
        itype = self.val.type.template_argument(0)
        # If the inferior program is compiled with -D_GLIBCXX_DEBUG
        # some of the internal implementation details change.
        if self.typename == 'std::_List_iterator' or \
                self.typename == 'std::_List_const_iterator':
            nodetype = gdb.lookup_type('std::_List_node<%s>' % print_type(itype)).pointer()
        elif self.typename == 'std::__norm::_List_iterator' or \
                self.typename == 'std::__norm::_List_const_iterator':
            nodetype = gdb.lookup_type(
                'std::__norm::_List_node<%s>' % print_type(itype)).pointer()
        else:
            raise ValueError(
                'Cannot cast list node for list iterator printer.')
        return self.val['_M_node'].cast(nodetype).dereference()['_M_data']


class StdSlistPrinter:
    'Print a __gnu_cxx::slist'

    class _iterator(Iterator):
        def __init__(self, nodetype, head):
            self.nodetype = nodetype
            self.base = head['_M_head']['_M_next']
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.base == 0:
                raise StopIteration
            elt = self.base.cast(self.nodetype).dereference()
            self.base = elt['_M_next']
            count = self.count
            self.count = self.count + 1
            return('[%d]' % count, elt['_M_data'])

    def __init__(self, typename, val):
        self.val = val

    def children(self):
        itype = self.val.type.template_argument(0)
        nodetype = gdb.lookup_type(
            '__gnu_cxx::_Slist_node<%s>' % print_type(itype)).pointer()
        return self._iterator(nodetype, self.val)

    def to_string(self):
        if self.val['_M_head']['_M_next'] == 0:
            return 'empty __gnu_cxx::slist'
        return '__gnu_cxx::slist'


class StdSlistIteratorPrinter:
    'Print __gnu_cxx::slist::iterator'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        itype = self.val.type.template_argument(0)
        nodetype = gdb.lookup_type(
            '__gnu_cxx::_Slist_node<%s>' % print_type(itype)).pointer()
        return self.val['_M_node'].cast(nodetype).dereference()['_M_data']


class StdVectorPrinter:
    'Print a std::vector'

    class _iterator(Iterator):
        def __init__(self, start, finish, bitvec):
            self.bitvec = bitvec
            if bitvec:
                self.item = start['_M_p']
                self.so = start['_M_offset']
                self.finish = finish['_M_p']
                self.fo = finish['_M_offset']
                itype = self.item.dereference().type
                self.isize = 8 * itype.sizeof
            else:
                self.item = start
                self.finish = finish
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            count = self.count
            self.count = self.count + 1
            if self.bitvec:
                if self.item == self.finish and self.so >= self.fo:
                    raise StopIteration
                elt = self.item.dereference()
                if elt & (1 << self.so):
                    obit = 1
                else:
                    obit = 0
                self.so = self.so + 1
                if self.so >= self.isize:
                    self.item = self.item + 1
                    self.so = 0
                return('[%d]' % count, obit)
            else:
                if self.item == self.finish:
                    raise StopIteration
                elt = self.item.dereference()
                self.item = self.item + 1
                return('[%d]' % count, elt)

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val
        self.is_bool = val.type.template_argument(0).code == gdb.TYPE_CODE_BOOL

    def children(self):
        return self._iterator(self.val['_M_impl']['_M_start'],
                              self.val['_M_impl']['_M_finish'],
                              self.is_bool)

    def to_string(self):
        start = self.val['_M_impl']['_M_start']
        finish = self.val['_M_impl']['_M_finish']
        end = self.val['_M_impl']['_M_end_of_storage']
        if self.is_bool:
            start = self.val['_M_impl']['_M_start']['_M_p']
            so = self.val['_M_impl']['_M_start']['_M_offset']
            finish = self.val['_M_impl']['_M_finish']['_M_p']
            fo = self.val['_M_impl']['_M_finish']['_M_offset']
            itype = start.dereference().type
            bl = 8 * itype.sizeof
            length = (bl - so) + bl * ((finish - start) - 1) + fo
            capacity = bl * (end - start)
            return('%s<bool> of length %d, capacity %d' % (
                self.typename, int(length), int(capacity)))
        else:
            return('%s of length %d, capacity %d' % (
                self.typename, int(finish - start), int(end - start)))

    def display_hint(self):
        return 'array'


class StdVectorIteratorPrinter:
    'Print std::vector::iterator'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        return self.val['_M_current'].dereference()


class StdTuplePrinter:
    'Print a std::tuple'

    class _iterator(Iterator):
        def __init__(self, head):
            self.head = head

            # Set the base class as the initial head of the
            # tuple.
            nodes = self.head.type.fields()
            if len(nodes) != 1:
                raise ValueError(
                    'Top of tuple tree does not consist of a single node.')

            # Set the actual head to the first pair.
            self.head = self.head.cast(nodes[0].type)
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            nodes = self.head.type.fields()
            # Check for further recursions in the inheritance tree.
            if len(nodes) == 0:
                raise StopIteration
            # Check that this iteration has an expected structure.
            if len(nodes) != 2:
                raise ValueError(
                    'Cannot parse more than 2 nodes in a tuple tree.')

            # - Left node is the next recursion parent.
            # - Right node is the actual class contained in the tuple.

            # Process right node.
            impl = self.head.cast(nodes[1].type)

            # Process left node and set it as head.
            self.head = self.head.cast(nodes[0].type)
            self.count = self.count + 1

            # Finally, check the implementation.  If it is
            # wrapped in _M_head_impl return that, otherwise return
            # the value 'as is'.
            fields = impl.type.fields()
            if len(fields) < 1 or fields[0].name != '_M_head_impl':
                return('[%d]' % self.count, impl)
            else:
                return('[%d]' % self.count, impl['_M_head_impl'])

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def children(self):
        return self._iterator(self.val)

    def to_string(self):
        return '%s containing' % (self.typename)


class StdStackOrQueuePrinter:
    'Print a std::stack or std::queue'

    def __init__(self, typename, val):
        self.typename = typename
        self.visualizer = gdb.default_visualizer(val['c'])

    def children(self):
        return self.visualizer.children()

    def to_string(self):
        return '%s wrapping: %s' % (self.typename,
                                    self.visualizer.to_string())

    def display_hint(self):
        if hasattr(self.visualizer, 'display_hint'):
            return self.visualizer.display_hint()
        return None


class RbtreeIterator(Iterator):
    def __init__(self, rbtree):
        self.size = rbtree['_M_t']['_M_impl']['_M_node_count']
        self.node = rbtree['_M_t']['_M_impl']['_M_header']['_M_left']
        self.count = 0

    def __iter__(self):
        return self

    def __len__(self):
        return int(self.size)

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.count == self.size:
            raise StopIteration
        result = self.node
        self.count = self.count + 1
        if self.count < self.size:
            # Compute the next node.
            node = self.node
            if node.dereference()['_M_right']:
                node = node.dereference()['_M_right']
                while node.dereference()['_M_left']:
                    node = node.dereference()['_M_left']
            else:
                parent = node.dereference()['_M_parent']
                while node == parent.dereference()['_M_right']:
                    node = parent
                    parent = parent.dereference()['_M_parent']
                if node.dereference()['_M_right'] != parent:
                    node = parent
            self.node = node
        return result


# This is a pretty printer for std::_Rb_tree_iterator (which is
# std::map::iterator), and has nothing to do with the RbtreeIterator
# class above.
class StdRbtreeIteratorPrinter:
    'Print std::map::iterator'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        valuetype = self.val.type.template_argument(0)
        nodetype = gdb.lookup_type('std::_Rb_tree_node < %s >' % print_type(valuetype))
        nodetype = nodetype.pointer()
        return self.val.cast(nodetype).dereference()['_M_value_field']


class StdDebugIteratorPrinter:
    'Print a debug enabled version of an iterator'

    def __init__(self, typename, val):
        self.val = val

    # Just strip away the encapsulating __gnu_debug::_Safe_iterator
    # and return the wrapped iterator value.
    def to_string(self):
        itype = self.val.type.template_argument(0)
        return self.val['_M_current'].cast(itype)


# Turn an RbtreeIterator into a pretty-print iterator.
class StdMapPrinter:
    'Print a std::map or std::multimap'

    class _iter(Iterator):
        def __init__(self, rbiter, type):
            self.rbiter = rbiter
            self.count = 0
            self.type = type

        def __iter__(self):
            return self

        def __next__(self):
            if self.count % 2 == 0:
                n = next(self.rbiter)
                n = n.cast(self.type).dereference()['_M_value_field']
                self.pair = n
                item = n['first']
            else:
                item = self.pair['second']
            result = ('[%d]' % self.count, item)
            self.count = self.count + 1
            return result

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        return '%s with %d elements' % (self.typename,
                                        len(RbtreeIterator(self.val)))

    def children(self):
        keytype = self.val.type.template_argument(0).const()
        valuetype = self.val.type.template_argument(1)
        nodetype = gdb.lookup_type(
            'std::_Rb_tree_node< std::pair< %s, %s > >' % (print_type(keytype), print_type(valuetype)))
        nodetype = nodetype.pointer()
        return self._iter(RbtreeIterator(self.val), nodetype)

    def display_hint(self):
        return 'map'


# Turn an RbtreeIterator into a pretty-print iterator.
class StdSetPrinter:
    'Print a std::set or std::multiset'

    class _iter(Iterator):
        def __init__(self, rbiter, type):
            self.rbiter = rbiter
            self.count = 0
            self.type = type

        def __iter__(self):
            return self

        def __next__(self):
            item = next(self.rbiter)
            item = item.cast(self.type).dereference()['_M_value_field']
            # FIXME: this is weird ... what to do?
            # Maybe a 'set' display hint?
            result = ('[%d]' % self.count, item)
            self.count = self.count + 1
            return result

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        return '%s with %d elements' % (self.typename,
                                        len(RbtreeIterator(self.val)))

    def children(self):
        keytype = self.val.type.template_argument(0)
        nodetype = gdb.lookup_type(
            'std::_Rb_tree_node< %s >' % print_type(keytype)).pointer()
        return self._iter(RbtreeIterator(self.val), nodetype)


class StdBitsetPrinter:
    'Print a std::bitset'

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        # If template_argument handled values, we could print the
        # size.  Or we could use a regexp on the type.
        return '%s' % (self.typename)

    def children(self):
        words = self.val['_M_w']
        wtype = words.type

        # The _M_w member can be either an unsigned long, or an
        # array.  This depends on the template specialization used.
        # If it is a single long, convert to a single element list.
        if wtype.code == gdb.TYPE_CODE_ARRAY:
            tsize = wtype.target().sizeof
        else:
            words = [words]
            tsize = wtype.sizeof

        nwords = wtype.sizeof / tsize
        result = []
        byte = 0
        while byte < nwords:
            w = words[byte]
            bit = 0
            while w != 0:
                if (w & 1) != 0:
                    # Another spot where we could use 'set'?
                    result.append(('[%d]' % (byte * tsize * 8 + bit), 1))
                bit = bit + 1
                w = w >> 1
            byte = byte + 1
        return result


class StdDequePrinter:
    'Print a std::deque'

    class _iter(Iterator):
        def __init__(self, node, start, end, last, buffer_size):
            self.node = node
            self.p = start
            self.end = end
            self.last = last
            self.buffer_size = buffer_size
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.p == self.last:
                raise StopIteration

            result = ('[%d]' % self.count, self.p.dereference())
            self.count = self.count + 1

            # Advance the 'cur' pointer.
            self.p = self.p + 1
            if self.p == self.end:
                # If we got to the end of this bucket, move to the
                # next bucket.
                self.node = self.node + 1
                self.p = self.node[0]
                self.end = self.p + self.buffer_size

            return result

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val
        self.elttype = val.type.template_argument(0)
        size = self.elttype.sizeof
        if size < 512:
            self.buffer_size = int(512 / size)
        else:
            self.buffer_size = 1

    def to_string(self):
        start = self.val['_M_impl']['_M_start']
        end = self.val['_M_impl']['_M_finish']

        delta_n = end['_M_node'] - start['_M_node'] - 1
        delta_s = start['_M_last'] - start['_M_cur']
        delta_e = end['_M_cur'] - end['_M_first']

        size = self.buffer_size * delta_n + delta_s + delta_e

        return '%s with %d elements' % (self.typename, int(size))

    def children(self):
        start = self.val['_M_impl']['_M_start']
        end = self.val['_M_impl']['_M_finish']
        return self._iter(start['_M_node'], start['_M_cur'], start['_M_last'],
                          end['_M_cur'], self.buffer_size)

    def display_hint(self):
        return 'array'


class StdDequeIteratorPrinter:
    'Print std::deque::iterator'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        return self.val['_M_cur'].dereference()


class StdStringPrinter:
    'Print a std::basic_string of some kind'

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        # Make sure &string works, too.
        type = self.val.type
        if type.code == gdb.TYPE_CODE_REF:
            type = type.target()

        # Calculate the length of the string so that to_string returns
        # the string according to length, not according to first null
        # encountered.
        ptr = self.val['_M_dataplus']['_M_p']
        realtype = type.unqualified().strip_typedefs()
        reptype = gdb.lookup_type(str(realtype) + '::_Rep').pointer()
        header = ptr.cast(reptype) - 1
        len = header.dereference()['_M_length']
        if hasattr(ptr, 'lazy_string'):
            return ptr.lazy_string(length=len)
        return ptr.string(length=len)

    def display_hint(self):
        return 'string'


class Tr1HashtableIterator(Iterator):
    def __init__(self, hash):
        self.count = 0
        self.n_buckets = hash['_M_element_count']
        if self.n_buckets == 0:
            self.node = False
        else:
            self.bucket = hash['_M_buckets']
            self.node = self.bucket[0]
            self.update()

    def __iter__(self):
        return self

    def update(self):
        # If we advanced off the end of the chain, move to the next
        # bucket.
        while self.node == 0:
            self.bucket = self.bucket + 1
            self.node = self.bucket[0]

        # If we advanced off the end of the bucket array, then
        # we're done.
        if self.count == self.n_buckets:
            self.node = False
        else:
            self.count = self.count + 1

    def __next__(self):
        if not self.node:
            raise StopIteration
        result = self.node.dereference()['_M_v']
        self.node = self.node.dereference()['_M_next']
        self.update()
        return result


class Tr1UnorderedSetPrinter:
    'Print a tr1::unordered_set'

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        return '%s with %d elements' % (
            self.typename, self.val['_M_element_count'])

    @staticmethod
    def format_count(i):
        return '[%d]' % i

    def children(self):
        counter = imap(self.format_count, itertools.count())
        return izip(counter, Tr1HashtableIterator(self.val))


class Tr1UnorderedMapPrinter:
    'Print a tr1::unordered_map'

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val

    def to_string(self):
        return '%s with %d elements' % (
            self.typename, self.val['_M_element_count'])

    @staticmethod
    def flatten(list):
        for elt in list:
            for i in elt:
                yield i

    @staticmethod
    def format_one(elt):
        return(elt['first'], elt['second'])

    @staticmethod
    def format_count(i):
        return '[%d]' % i

    def children(self):
        counter = imap(self.format_count, itertools.count())
        # Map over the hash table and flatten the result.
        data = self.flatten(imap(
            self.format_one, Tr1HashtableIterator(self.val)))
        # Zip the two iterators together.
        return izip(counter, data)

    def display_hint(self):
        return 'map'


class PairPrinter:
    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        return '(%s, %s)' % (self.val['first'], self.val['second'])

    def children(self):
        return [
            ('first', self.val['first']),
            ('second', self.val['second'])].__iter__()

    def display_hint(self):
        return 'pair'


# A 'regular expression' printer which conforms to the
# 'SubPrettyPrinter' protocol from gdb.printing.
class RxPrinter(object):
    def __init__(self, name, function):
        super(RxPrinter, self).__init__()
        self.name = name
        self.function = function
        self.enabled = True

    def invoke(self, value):
        if not self.enabled:
            return None
        return self.function(self.name, value)


# A pretty-printer that conforms to the 'PrettyPrinter' protocol from
# gdb.printing.  It can also be used directly as an old-style printer.
class Printer(object):
    def __init__(self, name):
        super(Printer, self).__init__()
        self.name = name
        self.subprinters = []
        self.lookup = {}
        self.enabled = True
        self.compiled_rx = re.compile('^([a-zA-Z0-9_:]+)<.*>$')

    def add(self, name, function):
        # A small sanity check.
        # FIXME
        if not self.compiled_rx.match(name + '<>'):
            raise ValueError(
                'libstdc++ programming error: \'%s\' does not match' % name)
        printer = RxPrinter(name, function)
        self.subprinters.append(printer)
        self.lookup[name] = printer

    @staticmethod
    def get_basic_type(type):
        # If it points to a reference, get the reference.
        if type.code == gdb.TYPE_CODE_REF:
            type = type.unqualified().target()

        # Get the unqualified type, stripped of typedefs.
        type = type.unqualified().strip_typedefs()

        return type.tag

    def __call__(self, val):
        if val.type.tag in self.lookup:
            try:
                return self.lookup[val.type.tag].invoke(val)
            except Exception:
                print('Printer initialization error: %s, %s' % sys.exc_info()[0:2])
                return None

        typename = self.get_basic_type(val.type)
        if not typename:
            return None
        if typename in self.lookup:
            try:
                return self.lookup[typename].invoke(val)
            except Exception:
                print('Printer initialization error: %s, %s' % sys.exc_info()[0:2])
                return None

        # All the types we match are template types, so we can use a
        # dictionary.
        match = self.compiled_rx.match(typename)
        if not match:
            return None

        basename = match.group(1)
        if basename in self.lookup:
            try:
                return self.lookup[basename].invoke(val)
            except Exception:
                print('Printer initialization error: %s, %s' % sys.exc_info()[0:2])
                return None

        # Cannot find a pretty printer.  Return None.
        return None


def build_printer():
    printer = Printer('libstdc++-v6')

    # libstdc++ objects requiring pretty-printing.
    # In order from:
    # http://gcc.gnu.org/onlinedocs/libstdc++/latest-doxygen/a01847.html
    printer.add('std::basic_string', StdStringPrinter)
    printer.add('std::bitset', StdBitsetPrinter)
    printer.add('std::deque', StdDequePrinter)
    printer.add('std::list', StdListPrinter)
    printer.add('std::map', StdMapPrinter)
    printer.add('std::multimap', StdMapPrinter)
    printer.add('std::multiset', StdSetPrinter)
    printer.add('std::priority_queue', StdStackOrQueuePrinter)
    printer.add('std::queue', StdStackOrQueuePrinter)
    printer.add('std::tuple', StdTuplePrinter)
    printer.add('std::set', StdSetPrinter)
    printer.add('std::stack', StdStackOrQueuePrinter)
    printer.add('std::unique_ptr', UniquePointerPrinter)
    printer.add('std::vector', StdVectorPrinter)

    # Printer registrations for classes compiled with -D_GLIBCXX_DEBUG.
    printer.add('std::__debug::bitset', StdBitsetPrinter)
    printer.add('std::__debug::deque', StdDequePrinter)
    printer.add('std::__debug::list', StdListPrinter)
    printer.add('std::__debug::map', StdMapPrinter)
    printer.add('std::__debug::multimap', StdMapPrinter)
    printer.add('std::__debug::multiset', StdSetPrinter)
    printer.add('std::__debug::priority_queue', StdStackOrQueuePrinter)
    printer.add('std::__debug::queue', StdStackOrQueuePrinter)
    printer.add('std::__debug::set', StdSetPrinter)
    printer.add('std::__debug::stack', StdStackOrQueuePrinter)
    printer.add('std::__debug::unique_ptr', UniquePointerPrinter)
    printer.add('std::__debug::vector', StdVectorPrinter)

    # These are the TR1 and C++0x printers.
    # For array - the default GDB pretty-printer seems reasonable.
    printer.add('std::shared_ptr', StdPointerPrinter)
    printer.add('std::weak_ptr', StdPointerPrinter)
    printer.add('std::unordered_map', Tr1UnorderedMapPrinter)
    printer.add('std::unordered_set', Tr1UnorderedSetPrinter)
    printer.add('std::unordered_multimap', Tr1UnorderedMapPrinter)
    printer.add('std::unordered_multiset', Tr1UnorderedSetPrinter)

    printer.add('std::tr1::shared_ptr', StdPointerPrinter)
    printer.add('std::tr1::weak_ptr', StdPointerPrinter)
    printer.add('std::tr1::unordered_map', Tr1UnorderedMapPrinter)
    printer.add('std::tr1::unordered_set', Tr1UnorderedSetPrinter)
    printer.add('std::tr1::unordered_multimap', Tr1UnorderedMapPrinter)
    printer.add('std::tr1::unordered_multiset', Tr1UnorderedSetPrinter)

    # These are the C++0x printer registrations for -D_GLIBCXX_DEBUG cases.
    # The tr1 namespace printers do not seem to have any debug
    # equivalents, so do no register them.
    printer.add('std::__debug::unordered_map', Tr1UnorderedMapPrinter)
    printer.add('std::__debug::unordered_set', Tr1UnorderedSetPrinter)
    printer.add('std::__debug::unordered_multimap', Tr1UnorderedMapPrinter)
    printer.add('std::__debug::unordered_multiset', Tr1UnorderedSetPrinter)

    # Extensions.
    printer.add('__gnu_cxx::slist', StdSlistPrinter)

    # These shouldn't be necessary, if GDB 'print *i' worked.
    # But it often doesn't, so here they are.
    printer.add('std::_List_iterator', StdListIteratorPrinter)
    printer.add('std::_List_const_iterator', StdListIteratorPrinter)
    printer.add('std::_Rb_tree_iterator', StdRbtreeIteratorPrinter)
    printer.add('std::_Rb_tree_const_iterator', StdRbtreeIteratorPrinter)
    printer.add('std::_Deque_iterator', StdDequeIteratorPrinter)
    printer.add('std::_Deque_const_iterator', StdDequeIteratorPrinter)
    printer.add('__gnu_cxx::__normal_iterator', StdVectorIteratorPrinter)
    printer.add('__gnu_cxx::_Slist_iterator', StdSlistIteratorPrinter)

    # Debug (compiled with -D_GLIBCXX_DEBUG) printer
    # registrations.  The Rb_tree debug iterator when unwrapped
    # from the encapsulating __gnu_debug::_Safe_iterator does not
    # have the __norm namespace. Just use the existing printer
    # registration for that.
    printer.add('__gnu_debug::_Safe_iterator', StdDebugIteratorPrinter)
    printer.add('std::__norm::_List_iterator', StdListIteratorPrinter)
    printer.add('std::__norm::_List_const_iterator', StdListIteratorPrinter)
    printer.add('std::__norm::_Deque_const_iterator', StdDequeIteratorPrinter)
    printer.add('std::__norm::_Deque_iterator', StdDequeIteratorPrinter)

    return printer

def register_printers(obj):
    global _use_gdb_pp

    p = build_printer()
    if _use_gdb_pp:
        gdb.printing.register_pretty_printer(obj, p)
    else:
        if obj is None:
            obj = gdb
        obj.pretty_printers.append(p)
