# Pretty-printers for libc++.

# Copyright (C) 2008-2013 Free Software Foundation, Inc.

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import gdb
import itertools
import re
import sys

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

_string_encoding = 'utf-8'

def print_type(type_obj):
    global _use_type_printing
    if not _use_type_printing:
        return str(type_obj)
    if type_obj.code == gdb.TYPE_CODE_PTR:
        return print_type(type_obj.target()) + '*'
    if type_obj.code == gdb.TYPE_CODE_ARRAY:
        type_str = print_type(type_obj.target())
        if str(type_obj.strip_typedefs()).endswith('[]'):
            return type_str + '[]' # array of unknown bound
        return "%s[%d]" % (type_str, type_obj.range()[1] + 1)
    if type_obj.code == gdb.TYPE_CODE_REF:
        return print_type(type_obj.target()) + '&'
    if hasattr(gdb, 'TYPE_CODE_RVALUE_REF'):
        if type_obj.code == gdb.TYPE_CODE_RVALUE_REF:
            return print_type(type_obj.target()) + '&&'

    return gdb.types.apply_type_recognizers(
        gdb.types.get_type_recognizers(), type_obj) or str(type_obj)

_versioned_namespace = 'std::__y1::'

def strip_versioned_namespace(typename):
    global _versioned_namespace
    if typename.startswith(_versioned_namespace):
        return 'std::' + typename[len(_versioned_namespace):]
    return typename

def get_template_arg_list(type_obj):
    "Return a type's template arguments as a list"
    n = 0
    template_args = []
    while True:
        try:
            template_args.append(type_obj.template_argument(n))
        except:
            return template_args
        n += 1

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

def get_variant_value(val):
    """
    val must be a std::variant.
    Returns (index, value) or (-1, None) if val is valueless.
    """
    impl = val['__impl_']
    index = impl['__index']
    valueless_index = (1 << (index.type.sizeof * 8)) - 1  # static_cast<_IndexType>(-1), assuming index is unsigned
    if index == valueless_index:
        return -1, None
    storage = impl['__data']  # A union. `__head` is (wrapped) ith value, `__tail` is another union.
    for _ in range(index):
        storage = storage['__tail']
    return index, storage['__head']['__value']

# Starting with the type ORIG, search for the member type NAME.  This
# handles searching upward through superclasses.  This is needed to
# work around http://sourceware.org/bugzilla/show_bug.cgi?id=13615.
def find_type(orig, name):
    typ = orig.strip_typedefs()
    while True:
        search = str(typ) + '::' + name
        try:
            return gdb.lookup_type(search)
        except RuntimeError:
            pass
        # The type was not found, so try the superclass.  We only need
        # to check the first superclass, so we don't bother with
        # anything fancier here.
        field = typ.fields()[0]
        if not field.is_base_class:
            raise ValueError("Cannot find type %s::%s" % (str(orig), name))
        typ = field.type

class StdStringPrinter:
    "Print a std::basic_string of some kind"

    def __init__(self, typename, val):
        self.val = val
        self.typename = typename

    def to_string(self):
        ss = destructure_compressed_pair(self.val['__r_'])[0]['__s']
        is_long = ss['__is_long_']
        if is_long:
            sl = destructure_compressed_pair(self.val['__r_'])[0]['__l']
            ptr = sl['__data_']
            size = sl['__size_']
        else:
            ptr = ss['__data_']
            size = ss['__size_']
        try:
            return ptr.string(length = size)
        except UnicodeDecodeError:  # Fallback for non-unicode strings
            char_array_ptr = gdb.lookup_type('char').array(size).pointer()
            return ptr.cast(char_array_ptr).dereference()

    def display_hint (self):
        return 'string'

class StdStringViewPrinter:
    "Print a std::basic_string_view of some kind"

    def __init__(self, typename, val):
        self.val = val
        self.typename = typename

    def to_string(self):
        # Make sure &string_view works, too.
        type = self.val.type
        if type.code == gdb.TYPE_CODE_REF:
            type = type.target()

        len = self.val['__size_']
        ptr = self.val['__data_']
        try:
            return ptr.string(length = len)
        except UnicodeDecodeError:  # Fallback for non-unicode strings
            char_array_ptr = gdb.lookup_type('char').array(len).pointer()
            return ptr.cast(char_array_ptr).dereference()

    def display_hint (self):
        return 'string'

class SharedPointerPrinter:
    "Print a shared_ptr or weak_ptr"

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val

    def children (self):
        return [('get()', self.val['__ptr_'])]

    def to_string (self):
        state = 'empty'
        refcounts = self.val['__cntrl_']
        if refcounts != 0:
            usecount = refcounts['__shared_owners_']['__a_']['__a_value'] + 1
            weakcount = refcounts['__shared_weak_owners_']['__a_']['__a_value']
            if usecount == 0:
                state = 'expired, weak %d' % weakcount
            else:
                state = 'count %d, weak %d' % (usecount, weakcount)

        return '%s (%s)' % (self.typename, state)

class UniquePointerPrinter:
    "Print a unique_ptr"

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val

    def children (self):
        v, _ = destructure_compressed_pair(self.val['__ptr_'])
        return [('get()', v)]

    def to_string (self):
        v, _ = destructure_compressed_pair(self.val['__ptr_'])
        return ('%s<%s>' % (str(self.typename), print_type(v.type.target())))

class AtomicPrinter:
    "Print atomic"

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val

    def children (self):
        return [('load()', self.val['__a_']['__a_value'])]

    def to_string (self):
        return 'std::atomic'

class StdPairPrinter:
    "Print a std::pair"

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val;

    def children (self):
        return [('first', self.val['first']), ('second', self.val['second'])]

    def to_string (self):
        return 'pair'

#    def display_hint(self):
#        return 'array'

class StdTuplePrinter:
    "Print a std::tuple"

    class _iterator(Iterator):
        def __init__ (self, tuple_):
            self.count = 0
            if tuple_.type.has_key('__base_'):
                self.base = tuple_['__base_']
                self.fields = self.base.type.fields()
            else:  # std::tuple<>
                self.fields = []

        def __iter__ (self):
            return self

        def __next__ (self):
            if self.count >= len(self.fields):
                raise StopIteration
            self.field = self.base.cast(self.fields[self.count].type)['__value_']
            self.count += 1
            return ('[%d]' % (self.count - 1), self.field)

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val;

    def children (self):
        return self._iterator (self.val)

    def to_string (self):
        if len (self.val.type.fields ()) == 0:
            return 'empty %s' % (self.typename)
        return 'tuple'

#    def display_hint(self):
#        return 'array'

class StdListPrinter:
    "Print a std::list"

    class _iterator(Iterator):
        def __init__(self, nodetype, head):
            self.nodetype = nodetype
            self.base = head['__next_']
            self.head = head.address
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.base == self.head:
                raise StopIteration
            elt = self.base.cast(self.nodetype).dereference()
            self.base = elt['__next_']
            count = self.count
            self.count = self.count + 1
            return ('[%d]' % count, elt['__value_'])

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val
        node = self.val["__end_"]
        self.nodetype = gdb.lookup_type(str(node.type.strip_typedefs()) + "::__node_pointer")

    def children(self):
        return self._iterator(self.nodetype, self.val['__end_'])

    def to_string(self):
        if self.val['__end_']['__next_'] == self.val['__end_'].address:
            return 'empty %s' % (self.typename)
        return '%s' % (self.typename)

#    def display_hint(self):
#        return 'array'

class StdListIteratorPrinter:
    "Print std::list::iterator"

    def __init__(self, typename, val):
        self.val = val
        self.typename = typename

    def to_string(self):
        return self.val['__ptr_']['__value_']

class StdForwardListPrinter:
    "Print a std::forward_list"

    class _iterator(Iterator):
        def __init__(self, head):
            self.node = head
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.node == 0:
                raise StopIteration

            result = ('[%d]' % self.count, self.node['__value_'])
            self.count += 1
            self.node = self.node['__next_']
            return result

    def __init__(self, typename, val):
        self.val = val
        self.typename = typename
        self.head = destructure_compressed_pair(val['__before_begin_'])[0]['__next_']

    def children(self):
        return self._iterator(self.head)

    def to_string(self):
        if self.head == 0:
            return 'empty %s' % (self.typename)
        return '%s' % (self.typename)

class StdVectorPrinter:
    "Print a std::vector"

    class _iterator(Iterator):
        def __init__ (self, start, finish_or_size, bits_per_word, bitvec):
            self.bitvec = bitvec
            if bitvec:
                self.item   = start
                self.so     = 0
                self.size   = finish_or_size
                self.bits_per_word = bits_per_word
            else:
                self.item = start
                self.finish = finish_or_size
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            count = self.count
            self.count = self.count + 1
            if self.bitvec:
                if count == self.size:
                    raise StopIteration
                elt = self.item.dereference()
                if elt & (1 << self.so):
                    obit = 1
                else:
                    obit = 0
                self.so = self.so + 1
                if self.so >= self.bits_per_word:
                    self.item = self.item + 1
                    self.so = 0
                return ('[%d]' % count, obit)
            else:
                if self.item == self.finish:
                    raise StopIteration
                elt = self.item.dereference()
                self.item = self.item + 1
                return ('[%d]' % count, elt)

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val
        self.is_bool = 0
        for f in val.type.fields():
            if f.name == '__bits_per_word':
                self.is_bool = 1

    def children(self):
        if self.is_bool:
            return self._iterator(self.val['__begin_'],
                              self.val['__size_'],
                              self.val['__bits_per_word'],
                              self.is_bool)
        else:
            return self._iterator(self.val['__begin_'],
                              self.val['__end_'],
                              0,
                              self.is_bool)

    def to_string(self):
        start = self.val['__begin_']
        if self.is_bool:
            length   = self.val['__size_']
            capacity = self.val['__cap_alloc_']['__first_'] * self.val['__bits_per_word']
            if length == 0:
                return 'empty %s<bool> (capacity=%d)' % (self.typename, int(capacity))
            else:
                return ('%s<bool> (length=%d, capacity=%d)' % (self.typename, int(length), int(capacity)))
        else:
            finish = self.val['__end_']
            end, _ = destructure_compressed_pair(self.val['__end_cap_'])
            length = finish - start
            capacity = end - start
            if length == 0:
                return 'empty %s (capacity=%d)' % (self.typename, int(capacity))
            else:
                return ('%s (length=%d, capacity=%d)' % (self.typename, int(length), int(capacity)))

#    def display_hint(self):
#        return 'array'

class StdVectorIteratorPrinter:
    "Print std::vector::iterator"

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        return self.val['__i'].dereference();

class StdVectorBoolIteratorPrinter:
    "Print std::vector<bool>::iterator"

    def __init__(self, typename, val):
        self.segment = val['__seg_'].dereference()
        self.ctz = val['__ctz_']

    def to_string(self):
        if self.segment & (1 << self.ctz):
            return 1
        else:
            return 0

class StdDequePrinter:
    "Print a std::deque"

    class _iterator(Iterator):
        def __init__(self, size, block_size, start, map_begin, map_end):
            self.block_size = block_size
            self.count = 0
            self.end_p = size + start
            self.end_mp = map_begin + self.end_p / block_size
            self.p = 0
            self.mp = map_begin + start / block_size
            if map_begin == map_end:
                self.p = 0
                self.end_p = 0
            else:
                self.p = self.mp.dereference() + start % block_size
                self.end_p = self.end_mp.dereference() + self.end_p % block_size

        def __iter__(self):
            return self

        def __next__(self):
            old_p = self.p

            self.count += 1
            self.p += 1;
            if (self.p - self.mp.dereference()) == self.block_size:
                self.mp += 1
                self.p = self.mp.dereference()

            if (self.mp > self.end_mp) or ((self.p > self.end_p) and (self.mp == self.end_mp)):
                raise StopIteration

            return ('[%d]' % int(self.count - 1), old_p.dereference())

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val
        self.size, _ = destructure_compressed_pair(val['__size_'])

    def to_string(self):
        if self.size == 0:
            return 'empty %s' % self.typename
        else:
            return '%s (size=%d)' % (self.typename, long(self.size))

    def children(self):
        block_map = self.val['__map_']
        block_size = self.val['__block_size']
        if block_size.is_optimized_out:
            deque_type_name = self.val.type.strip_typedefs().name
            iter_type = gdb.lookup_type(deque_type_name + '::iterator')
            block_size = int(iter_type.template_argument(5))
            assert block_size != 0, 'deque block_size is optimized out and unrecoverable'
        return self._iterator(self.size, block_size,
                              self.val['__start_'], block_map['__begin_'],
                              block_map['__end_'])

#    def display_hint (self):
#        return 'array'

class StdDequeIteratorPrinter:
    "Print std::deque::iterator"

    def __init__(self, typename, val):
        self.val = val

    def to_string(self):
        return self.val['__ptr_'].dereference()

class StdStackOrQueuePrinter:
    "Print a std::stack or std::queue"

    def __init__ (self, typename, val):
        self.typename = typename
        self.visualizer = gdb.default_visualizer(val['c'])

    def children (self):
        return self.visualizer.children()

    def to_string (self):
        return '%s = %s' % (self.typename, self.visualizer.to_string())

    def display_hint (self):
        if hasattr (self.visualizer, 'display_hint'):
            return self.visualizer.display_hint ()
        return None

class StdBitsetPrinter:
    "Print a std::bitset"

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val
        self.bit_count = val.type.template_argument(0)

    def to_string (self):
        return '%s (length=%d)' % (self.typename, self.bit_count)

    def children (self):
        word = words = self.val['__first_']
        words_count = self.val['__n_words']
        bits_per_word = self.val['__bits_per_word']
        word_index = 0
        result = []

        for word_index in range(words_count):
            if words_count > 1:
                word = words[word_index]

            bit_index = 0
            while word != 0:
                if (word & 0x1) != 0:
                    result.append(('[%d]' % (word_index * bits_per_word + bit_index), 1))
                word >>= 1
                bit_index += 1

        return result

class StdSetPrinter:
    "Print a std::set or std::multiset"

    # Turn an RbtreeIterator into a pretty-print iterator.
    class _iterator(Iterator):
        def __init__(self, rbiter):
            self.rbiter = rbiter
            self.count = 0

        def __iter__(self):
            return self

        def __len__(self):
            return len(self.rbiter)

        def __next__(self):
            item = next(self.rbiter)
            item = item.dereference()['__value_']
            result = (('[%d]' % self.count), item)
            self.count += 1
            return result

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val
        self.rbiter = RbtreeIterator(self.val['__tree_'])

    def to_string (self):
        length = len(self.rbiter)
        if length == 0:
            return 'empty %s' % self.typename
        else:
            return '%s (count=%d)' % (self.typename, int(length))

    def children (self):
        return self._iterator(self.rbiter)

#    def display_hint (self):
#        return 'set'

class RbtreeIterator(Iterator):
    def __init__(self, rbtree):
        tree_type_name = rbtree.type.strip_typedefs().name
        self.node_type = gdb.lookup_type(tree_type_name + '::__node_pointer')
        self.node = rbtree['__begin_node_'].cast(self.node_type)
        self.size, _ = destructure_compressed_pair(rbtree['__pair3_'])
        self.count = 0

    def __iter__(self):
        return self

    def __len__(self):
        return int (self.size)

    def __next__(self):
        if self.count == self.size:
            raise StopIteration

        def deref(node, direction):
            return node.dereference()[direction].cast(self.node_type)

        result = self.node
        self.count += 1
        if self.count < self.size:
            # Compute the next node.
            node = self.node
            if deref(node, '__right_'):
                node = deref(node, '__right_')
                while deref(node, '__left_'):
                    node = deref(node, '__left_')
            else:
                parent_node = deref(node, '__parent_')
                while node != deref(parent_node, '__left_'):
                    node = parent_node
                    parent_node = deref(parent_node, '__parent_')
                node = parent_node

            self.node = node
        return result

class StdRbtreeIteratorPrinter:
    "Print std::set::iterator"

    def __init__ (self, typename, val):
        self.val = val

    def to_string (self):
        node_type_ptr = self.val.type.template_argument(1)
        return self.val['__ptr_'].cast(node_type_ptr).dereference()['__value_']

class StdMapPrinter:
    "Print a std::map or std::multimap"

    # Turn an RbtreeIterator into a pretty-print iterator.
    class _iterator(Iterator):
        def __init__(self, rbiter):
            self.rbiter = rbiter
            self.count = 0

        def __iter__(self):
            return self

        def __len__(self):
            return len(self.rbiter)

        def __next__(self):
            item = next(self.rbiter)
            item = item.dereference()['__value_']
            result = ('[%d] %s' % (self.count, str(item['__cc_']['first'])), item['__cc_']['second'])
            self.count += 1
            return result

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val
        self.rbiter = RbtreeIterator(val['__tree_'])

    def to_string (self):
        length = len(self.rbiter)
        if length == 0:
            return 'empty %s' % self.typename
        else:
            return '%s (count=%d)' % (self.typename, int(length))

    def children (self):
        return self._iterator(self.rbiter)

#    def display_hint (self):
#        return 'map'

class StdMapIteratorPrinter:
    "Print std::map::iterator"

    def __init__ (self, typename, val):
        self.val = val

    def to_string (self):
        internal = self.val['__i_']
        node_ptr_type = internal.type.template_argument(1)
        entry = internal['__ptr_'].cast(node_ptr_type).dereference()['__value_']['__cc_']
        return '[%s] %s' % (entry['first'], entry['second'])

class HashtableIterator(Iterator):
    def __init__ (self, hashtable):
        self.node_ptr_type = destructure_compressed_pair(hashtable['__p1_'])[0].type.template_argument(0)
        self.node = destructure_compressed_pair(hashtable['__p1_'])[0]['__next_']
        self.size, _ = destructure_compressed_pair(hashtable['__p2_'])

    def __iter__ (self):
        return self

    def __len__(self):
        return self.size

    def __next__ (self):
        if self.node == 0:
            raise StopIteration

        node = self.node.cast(self.node_ptr_type).dereference()
        result = node['__value_']
        self.node = node['__next_']
        return result

class StdHashtableIteratorPrinter:
    "Print std::unordered_set::iterator"

    def __init__ (self, typename, val):
        self.val = val

    def to_string (self):
        node_ptr_type=self.val.type.strip_typedefs().template_argument(0)
        return self.val['__node_'].cast(node_ptr_type).dereference()['__value_']

class StdUnorderedMapIteratorPrinter:
    "Print std::unordered_map::iterator"

    def __init__ (self, typename, val):
        self.val = val

    def to_string (self):
        internal = self.val['__i_']
        node_ptr_type = internal.type.template_argument(0)
        entry = internal['__node_'].cast(node_ptr_type).dereference()['__value_']['__cc_']
        return '[%s] %s' % (entry['first'], entry['second'])

class UnorderedSetPrinter:
    "Print a std::unordered_set"

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val
        self.hashtable = val['__table_']
        self.size, _ = destructure_compressed_pair(self.hashtable['__p2_'])
        self.hashtableiter = HashtableIterator(self.hashtable)

    def hashtable (self):
        return self.hashtable

    def to_string (self):
        if self.size == 0:
            return 'empty %s' % self.typename
        else:
            return '%s (count=%d)' % (self.typename, self.size)

    @staticmethod
    def format_count (i):
        return '[%d]' % i

    def children (self):
        counter = imap (self.format_count, itertools.count())
        return izip (counter, self.hashtableiter)

class UnorderedMapPrinter:
    "Print a std::unordered_map"

    def __init__ (self, typename, val):
        self.typename = typename
        self.val = val
        self.hashtable = val['__table_']
        self.size, _ = destructure_compressed_pair(self.hashtable['__p2_'])
        self.hashtableiter = HashtableIterator(self.hashtable)

    def hashtable (self):
        return self.hashtable

    def to_string (self):
        if self.size == 0:
            return 'empty %s' % self.typename
        else:
            return '%s (count=%d)' % (self.typename, self.size)

    def children (self):
        result = []
        count = 0
        for elt in self.hashtableiter:
            result.append(('[%d] %s' % (count, elt['__cc_']['first']), elt['__cc_']['second']))
            count += 1
        return result

#    def display_hint (self):
#        return 'map'

class StdOptionalPrinter:
    "Print std::optional"

    def __init__(self, typename, val):
        self.typename = typename
        if val['__engaged_']:
            self.val = val['__val_'].address.dereference()
        else:
            self.val = None

    def children(self):
        return [] if self.val is None else [('value()', self.val)]

    def to_string(self):
        return 'std::optional' + (' empty' if self.val is None else '')


class StdThreadPrinter:
    "Print std::thread"

    def __init__(self, typename, val):
        self.val = val['__t_']

    def children(self):
        if self.val == 0:
            return []
        inferior = gdb.selected_inferior()
        if inferior is None:
            return []
        if hasattr(inferior, 'thread_from_handle'):
            thread = inferior.thread_from_handle(self.val)
        else:
            thread = inferior.thread_from_thread_handle(self.val)
        if thread is not None and thread.is_valid():
            return [('pthread_t', self.val), ('lwpid', thread.ptid[1]), ('name', thread.name), ('num', thread.num)]
        return []

    def to_string(self):
        return 'std::thread'

# A "regular expression" printer which conforms to the
# "SubPrettyPrinter" protocol from gdb.printing.
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

class StdVariantPrinter(object):
    'Print a std::variant'

    def __init__(self, typename, val):
        self.typename = typename
        self.val = val
        index, item = get_variant_value(val)
        if index != -1:
            self.itemtype = val.type.template_argument(index)
            self.item = item
        else:
            self.itemtype = None
            self.item = None

    def children(self):
        return [] if self.item is None else [('%s' % print_type(self.itemtype), self.item)]

    def to_string(self):
        return self.val.type.name + (
                ' (valueless by exception)' if self.item is None else '')


# A pretty-printer that conforms to the "PrettyPrinter" protocol from
# gdb.printing.  It can also be used directly as an old-style printer.
class Printer(object):
    def __init__(self, name):
        super(Printer, self).__init__()
        self.name = name
        self.subprinters = []
        self.lookup = {}
        self.simple_lookup = {}
        self.enabled = True
        self.compiled_rx = re.compile('^([a-zA-Z0-9_:]+)<.*>$')

    def add(self, name, function):
        # A small sanity check.
        # FIXME
        if not self.compiled_rx.match(name + '<>'):
            raise ValueError('libstdc++ programming error: "%s" does not match' % name)
        printer = RxPrinter(name, function)
        self.subprinters.append(printer)
        self.lookup[name] = printer

    def add_simple(self, name, function):
        printer = RxPrinter(name, function)
        self.subprinters.append(printer)
        self.simple_lookup[name] = printer

    def add_version(self, base, name, function):
        global _versioned_namespace
        self.add(base + name, function)
        self.add(_versioned_namespace + name, function)

    def add_simple_version(self, base, name, function):
        global _versioned_namespace
        self.add_simple(base + name, function)
        self.add_simple(_versioned_namespace + name, function)

    @staticmethod
    def get_basic_type(type):
        # If it points to a reference, get the reference.
        if type.code == gdb.TYPE_CODE_REF:
            type = type.target ()

        # Get the unqualified type, stripped of typedefs.
        type = type.unqualified ().strip_typedefs ()

        return type.tag

    def __call__(self, val):
        typename = self.get_basic_type(val.type)
        if not typename:
            return None

        # All the types we match are template types, so we can use a
        # dictionary.
        match = self.compiled_rx.match(typename)
        if not match:
            if typename in self.simple_lookup:
                return self.simple_lookup[typename].invoke(val)
            return None

        basename = match.group(1)
        if basename in self.lookup:
            return self.lookup[basename].invoke(val)

        # Cannot find a pretty printer.  Return None.
        return None

libcxx_printer = None

class TemplateTypePrinter(object):
    r"""
    A type printer for class templates with default template arguments.

    Recognizes specializations of class templates and prints them without
    any template arguments that use a default template argument.
    Type printers are recursively applied to the template arguments.

    e.g. replace "std::vector<T, std::allocator<T> >" with "std::vector<T>".
    """

    def __init__(self, name, defargs):
        self.name = name
        self.defargs = defargs
        self.enabled = True

    class _recognizer(object):
        "The recognizer class for TemplateTypePrinter."

        def __init__(self, name, defargs):
            self.name = name
            self.defargs = defargs

        def recognize(self, type_obj):
            """
            If type_obj is a specialization of self.name that uses all the
            default template arguments for the class template, then return
            a string representation of the type without default arguments.
            Otherwise, return None.
            """

            if type_obj.tag is None:
                return None

            if not type_obj.tag.startswith(self.name):
                return None

            template_args = get_template_arg_list(type_obj)
            displayed_args = []
            require_defaulted = False
            for n in range(len(template_args)):
                # The actual template argument in the type:
                targ = template_args[n]
                # The default template argument for the class template:
                defarg = self.defargs.get(n)
                if defarg is not None:
                    # Substitute other template arguments into the default:
                    defarg = defarg.format(*template_args)
                    # Fail to recognize the type (by returning None)
                    # unless the actual argument is the same as the default.
                    try:
                        if targ != gdb.lookup_type(defarg):
                            return None
                    except gdb.error:
                        # Type lookup failed, just use string comparison:
                        if targ.tag != defarg:
                            return None
                    # All subsequent args must have defaults:
                    require_defaulted = True
                elif require_defaulted:
                    return None
                else:
                    # Recursively apply recognizers to the template argument
                    # and add it to the arguments that will be displayed:
                    displayed_args.append(print_type(targ))

            # This assumes no class templates in the nested-name-specifier:
            template_name = type_obj.tag[0:type_obj.tag.find('<')]
            template_name = strip_versioned_namespace(template_name)
            return template_name + '<' + ', '.join(displayed_args) + '>'

    def instantiate(self):
        "Return a recognizer object for this type printer."
        return self._recognizer(self.name, self.defargs)

def add_one_template_type_printer(obj, name, defargs):
    r"""
    Add a type printer for a class template with default template arguments.

    Args:
        name (str): The template-name of the class template.
        defargs (dict int:string) The default template arguments.

    Types in defargs can refer to the Nth template-argument using {N}
    (with zero-based indices).

    e.g. 'unordered_map' has these defargs:
    { 2: 'std::hash<{0}>',
      3: 'std::equal_to<{0}>',
      4: 'std::allocator<std::pair<const {0}, {1}> >' }

    """
    global _versioned_namespace
    defargs = { n: d.replace('std::', _versioned_namespace) for n,d in defargs.items() }
    printer = TemplateTypePrinter(_versioned_namespace + name, defargs)
    gdb.types.register_type_printer(obj, printer)

class FilteringTypePrinter(object):
    def __init__(self, match, name):
        self.match = match
        self.name = name
        self.enabled = True

    class _recognizer(object):
        def __init__(self, match, name):
            self.match = match
            self.name = name
            self.stripped_name = strip_versioned_namespace(name)
            self.type_obj = None

        def recognize(self, type_obj):
            if type_obj.tag is None:
                return None

            if self.type_obj is None:
                if not self.match in type_obj.tag:
                    # Filter didn't match.
                    return None
                try:
                    self.type_obj = gdb.lookup_type(self.name).strip_typedefs()
                except:
                    pass
            if self.type_obj == type_obj:
                return self.stripped_name
            return None

    def instantiate(self):
        return self._recognizer(self.match, self.name)

def add_one_type_printer(obj, match, name):
    global _versioned_namespace
    printer = FilteringTypePrinter(match, _versioned_namespace + name)
    gdb.types.register_type_printer(obj, printer)

def register_type_printers(obj):
    global _use_type_printing

    if not _use_type_printing:
        return

    # Add type printers for typedefs std::string, std::wstring etc.
    for ch in ('', 'w', 'u16', 'u32'):
        add_one_type_printer(obj, 'basic_string', ch + 'string')
        add_one_type_printer(obj, 'basic_string_view', ch + 'string_view')

    # Add type printers for typedefs std::istream, std::wistream etc.
    for ch in ('', 'w'):
        for x in ('ios', 'streambuf', 'istream', 'ostream', 'iostream',
                  'filebuf', 'ifstream', 'ofstream', 'fstream'):
            add_one_type_printer(obj, 'basic_' + x, ch + x)
        for x in ('stringbuf', 'istringstream', 'ostringstream',
                  'stringstream'):
            add_one_type_printer(obj, 'basic_' + x, ch + x)

    for ch in ('', 'w'):
        add_one_type_printer(obj, 'basic_regex', ch + 'regex')
    for ch in ('c', 's', 'wc', 'ws'):
        add_one_type_printer(obj, 'match_results', ch + 'match')
        for x in ('sub_match', 'regex_iterator', 'regex_token_iterator'):
            add_one_type_printer(obj, x, ch + x)

    # Note that we can't have a printer for std::wstreampos, because
    # it shares the same underlying type as std::streampos.
    add_one_type_printer(obj, 'fpos', 'streampos')

    for dur in ('nanoseconds', 'microseconds', 'milliseconds',
                'seconds', 'minutes', 'hours'):
        add_one_type_printer(obj, 'duration', dur)

    add_one_type_printer(obj, 'linear_congruential_engine', 'minstd_rand0')
    add_one_type_printer(obj, 'linear_congruential_engine', 'minstd_rand')
    add_one_type_printer(obj, 'mersenne_twister_engine', 'mt19937')
    add_one_type_printer(obj, 'mersenne_twister_engine', 'mt19937_64')
    add_one_type_printer(obj, 'subtract_with_carry_engine', 'ranlux24_base')
    add_one_type_printer(obj, 'subtract_with_carry_engine', 'ranlux48_base')
    add_one_type_printer(obj, 'discard_block_engine', 'ranlux24')
    add_one_type_printer(obj, 'discard_block_engine', 'ranlux48')
    add_one_type_printer(obj, 'shuffle_order_engine', 'knuth_b')

    # Do not show defaulted template arguments in class templates.
    add_one_template_type_printer(obj, 'unique_ptr',
            { 1: 'std::default_delete<{0}>' })
    add_one_template_type_printer(obj, 'deque', { 1: 'std::allocator<{0}>'})
    add_one_template_type_printer(obj, 'forward_list', { 1: 'std::allocator<{0}>'})
    add_one_template_type_printer(obj, 'list', { 1: 'std::allocator<{0}>'})
    add_one_template_type_printer(obj, 'vector', { 1: 'std::allocator<{0}>'})
    add_one_template_type_printer(obj, 'map',
            { 2: 'std::less<{0}>',
              3: 'std::allocator<std::pair<{0} const, {1}>>' })
    add_one_template_type_printer(obj, 'multimap',
            { 2: 'std::less<{0}>',
              3: 'std::allocator<std::pair<{0} const, {1}>>' })
    add_one_template_type_printer(obj, 'set',
            { 1: 'std::less<{0}>', 2: 'std::allocator<{0}>' })
    add_one_template_type_printer(obj, 'multiset',
            { 1: 'std::less<{0}>', 2: 'std::allocator<{0}>' })
    add_one_template_type_printer(obj, 'unordered_map',
            { 2: 'std::hash<{0}>',
              3: 'std::equal_to<{0}>',
              4: 'std::allocator<std::pair<{0} const, {1}>>'})
    add_one_template_type_printer(obj, 'unordered_multimap',
            { 2: 'std::hash<{0}>',
              3: 'std::equal_to<{0}>',
              4: 'std::allocator<std::pair<{0} const, {1}>>'})
    add_one_template_type_printer(obj, 'unordered_set',
            { 1: 'std::hash<{0}>',
              2: 'std::equal_to<{0}>',
              3: 'std::allocator<{0}>'})
    add_one_template_type_printer(obj, 'unordered_multiset',
            { 1: 'std::hash<{0}>',
              2: 'std::equal_to<{0}>',
              3: 'std::allocator<{0}>'})

def register_libcxx_printers (obj):
    "Register libc++ pretty-printers with objfile Obj."

    global _use_gdb_pp
    global libcxx_printer

    if _use_gdb_pp:
        gdb.printing.register_pretty_printer(obj, libcxx_printer)
    else:
        if obj is None:
            obj = gdb
        obj.pretty_printers.append(libcxx_printer)

    register_type_printers(obj)

def build_libcxx_dictionary ():
    global libcxx_printer

    libcxx_printer = Printer("libc++-v1")

    libcxx_printer.add_version('std::', 'basic_string_view', StdStringViewPrinter)
    libcxx_printer.add_version('std::', 'basic_string', StdStringPrinter)
    libcxx_printer.add_version('std::', 'bitset', StdBitsetPrinter)
    libcxx_printer.add_version('std::', 'deque', StdDequePrinter)
    libcxx_printer.add_version('std::', 'list', StdListPrinter)
    libcxx_printer.add_version('std::', 'map', StdMapPrinter)
    libcxx_printer.add_version('std::', 'multimap', StdMapPrinter)
    libcxx_printer.add_version('std::', 'multiset', StdSetPrinter)
    libcxx_printer.add_version('std::', 'priority_queue',
                               StdStackOrQueuePrinter)
    libcxx_printer.add_version('std::', 'queue', StdStackOrQueuePrinter)
    libcxx_printer.add_version('std::', 'tuple', StdTuplePrinter)
    libcxx_printer.add_version('std::', 'pair', StdPairPrinter)
    libcxx_printer.add_version('std::', 'set', StdSetPrinter)
    libcxx_printer.add_version('std::', 'stack', StdStackOrQueuePrinter)
    libcxx_printer.add_version('std::', 'unique_ptr', UniquePointerPrinter)
    libcxx_printer.add_version('std::', 'atomic', AtomicPrinter)
    libcxx_printer.add_version('std::', 'vector', StdVectorPrinter)
    # vector<bool>

    # For array - the default GDB pretty-printer seems reasonable.
    libcxx_printer.add_version('std::', 'shared_ptr', SharedPointerPrinter)
    libcxx_printer.add_version('std::', 'weak_ptr', SharedPointerPrinter)
    libcxx_printer.add_version('std::', 'unordered_map', UnorderedMapPrinter)
    libcxx_printer.add_version('std::', 'unordered_set', UnorderedSetPrinter)
    libcxx_printer.add_version('std::', 'unordered_multimap',
                               UnorderedMapPrinter)
    libcxx_printer.add_version('std::', 'unordered_multiset',
                               UnorderedSetPrinter)
    libcxx_printer.add_version('std::', 'forward_list', StdForwardListPrinter)
    libcxx_printer.add_version('std::', 'optional', StdOptionalPrinter)
    libcxx_printer.add_simple_version('std::', 'thread', StdThreadPrinter)
    libcxx_printer.add_version('std::', 'variant', StdVariantPrinter)

    libcxx_printer.add_version('std::', '__list_iterator',
                               StdListIteratorPrinter)
    libcxx_printer.add_version('std::', '__list_const_iterator',
                               StdListIteratorPrinter)
    libcxx_printer.add_version('std::', '__tree_iterator',
                               StdRbtreeIteratorPrinter)
    libcxx_printer.add_version('std::', '__tree_const_iterator',
                               StdRbtreeIteratorPrinter)
    libcxx_printer.add_version('std::', '__hash_iterator',
                               StdHashtableIteratorPrinter)
    libcxx_printer.add_version('std::', '__hash_const_iterator',
                               StdHashtableIteratorPrinter)
    libcxx_printer.add_version('std::', '__hash_map_iterator',
                               StdUnorderedMapIteratorPrinter)
    libcxx_printer.add_version('std::', '__hash_map_const_iterator',
                               StdUnorderedMapIteratorPrinter)
    libcxx_printer.add_version('std::', '__map_iterator',
                               StdMapIteratorPrinter)
    libcxx_printer.add_version('std::', '__map_const_iterator',
                               StdMapIteratorPrinter)
    libcxx_printer.add_version('std::', '__deque_iterator',
                               StdDequeIteratorPrinter)
    libcxx_printer.add_version('std::', '__wrap_iter',
                               StdVectorIteratorPrinter)
    libcxx_printer.add_version('std::', '__bit_iterator',
                               StdVectorBoolIteratorPrinter)


build_libcxx_dictionary ()
