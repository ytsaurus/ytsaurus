import gdb
import gdb.xmethod
import re
from libcxx_printers import destructure_compressed_pair

_versioned_namespace = 'std::__y1::'

class ArraySizeWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type, size):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type
        self.result_type = gdb.lookup_type('size_t')
        self.size = size

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj):
        return self.size


class ArrayIndexWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type, size):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type
        self.index_type = gdb.lookup_type('size_t')

    def get_arg_types(self):
        return self.index_type

    def get_result_type(self):
        return self.element_type.reference()

    def __call__(self, obj, index):
        return obj['__elems_'][index].reference_value()


class ArrayDataWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type, size):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.result_type = element_type.pointer()

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj):
        return obj['__elems_'].address.cast(self.result_type)


class VectorSizeWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type
        self.result_type = gdb.lookup_type('size_t')

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj):
        return obj['__end_'] - obj['__begin_']


class VectorIndexWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type
        self.index_type = gdb.lookup_type('size_t')

    def get_arg_types(self):
        return self.index_type

    def get_result_type(self):
        return self.element_type.reference()

    def __call__(self, obj, index):
        return obj['__begin_'][index].reference_value()


class VectorDataWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.result_type = element_type.pointer()

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj):
        return obj['__begin_'].cast(self.result_type)


class StringSizeWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type
        self.result_type = gdb.lookup_type('size_t')

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj):
        ss = destructure_compressed_pair(obj['__r_'])[0]['__s']
        is_long = ss['__is_long_']
        if is_long:
            sl = destructure_compressed_pair(obj['__r_'])[0]['__l']
            return sl['__size_']
        else:
            return ss['__size_']


class StringIndexWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.index_type = gdb.lookup_type('size_t')
        self.result_type = element_type.reference()

    def get_arg_types(self):
        return self.index_type

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj, idx):
        ss = destructure_compressed_pair(obj['__r_'])[0]['__s']
        __short_mask = 0x1
        if ((ss['__size_'] & __short_mask) == 0):
            return ss['__data_'][idx].reference_value().cast(self.result_type)
        else:
            sl = destructure_compressed_pair(obj['__r_'])[0]['__l']
            return sl['__data_'][idx].cast(self.result_type)


class StringDataWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.result_type = element_type.pointer()

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj):
        ss = destructure_compressed_pair(obj['__r_'])[0]['__s']
        __short_mask = 0x1
        if ((ss['__size_'] & __short_mask) == 0):
            return ss['__data_'].address.cast(self.result_type)
        else:
            sl = destructure_compressed_pair(obj['__r_'])[0]['__l']
            return sl['__data_'].address.cast(self.result_type)


class StringCStrWorker(StringDataWorker):
    def __init__(self, class_type, element_type):
        StringDataWorker.__init__(self, class_type, element_type)
        self.result_type = element_type.const().pointer()


class VectorMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'std::vector')
        self.class_name = _versioned_namespace + 'vector'
        self.method_workers = {
            'size': VectorSizeWorker,
            'operator[]': VectorIndexWorker,
            'data': VectorDataWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            element_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, element_type)


class ArrayMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'std::array')
        self.class_name = _versioned_namespace + 'array'
        self.method_workers = {
            'size': ArraySizeWorker,
            'operator[]': ArrayIndexWorker,
            'data': ArrayDataWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            element_type = class_type.template_argument(0)
            size = class_type.template_argument(1)
        except:
            return None
        return method(class_type, element_type, size)


class StringMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'std::string')
        self.class_name = _versioned_namespace + 'basic_string'
        self.method_workers = {
            'size': StringSizeWorker,
            'length': StringSizeWorker,
            'operator[]': StringIndexWorker,
            'data': StringDataWorker,
            'c_str': StringCStrWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            element_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, element_type)


class UniquePtrGetWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, value_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.value_type = value_type

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.value_type.pointer()

    def __call__(self, obj):
        return destructure_compressed_pair(obj['__ptr_'])[0]


class UniquePtrGetRefWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, value_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.value_type = value_type

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.value_type.reference()

    def __call__(self, obj):
        return destructure_compressed_pair(obj['__ptr_'])[0].dereference().reference_value()


class UniquePtrMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'std::unique_ptr')
        self.class_name = _versioned_namespace + 'unique_ptr'
        self.method_workers = {
            'get': UniquePtrGetWorker,
            'operator->': UniquePtrGetWorker,
            'get_ref': UniquePtrGetRefWorker,
            'operator*': UniquePtrGetRefWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            value_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, value_type)


class SharedPtrGetWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, value_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.value_type = value_type

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.value_type

    def __call__(self, obj):
        return obj['__ptr_'].cast(self.value_type.pointer())


class SharedPtrGetRefWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, value_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.value_type = value_type

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.value_type.reference()

    def __call__(self, obj):
        return obj['__ptr_'].cast(self.value_type.pointer()).dereference().reference_value()


class SharedPtrMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'std::shared_ptr')
        self.class_name = _versioned_namespace + 'shared_ptr'
        self.method_workers = {
            'get': SharedPtrGetWorker,
            'operator->': SharedPtrGetWorker,
            'get_ref': SharedPtrGetRefWorker,
            'operator*': SharedPtrGetRefWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            value_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, value_type)


class DequeSizeWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.result_type = gdb.lookup_type('size_t')

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj):
        return destructure_compressed_pair(obj['__size_'])[0]


class DequeIndexWorker(gdb.xmethod.XMethodMatcher):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type
        self.result_type = element_type.reference()
        self.index_type = gdb.lookup_type('size_t')

    def get_arg_types(self):
        return self.index_type

    def get_result_type(self):
        return self.result_type

    def __call__(self, obj, idx):
        idx2 = idx + obj['__start_']
        buf_size = 64 * self.index_type.sizeof
        block_size = int(max(2, buf_size / self.element_type.sizeof))
        block_idx = int(idx2 / block_size)
        block_ptr = (obj['__map_']['__begin_'] + block_idx).dereference()
        return (block_ptr + idx2 % block_size).dereference().reference_value()



class DequeMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'std::deque')
        self.class_name = _versioned_namespace + 'deque'
        self.method_workers = {
            'size': DequeSizeWorker,
            'operator[]': DequeIndexWorker,
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            value_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, value_type)

class OptionalValueWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.element_type.reference()

    def __call__(self, obj):
        if obj['__engaged_']:
            return obj['__val_'].reference_value()
        raise Exception('Trying to access data of empty std::optional')

class OptionalValuePointerWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.element_type.pointer()

    def __call__(self, obj):
        if obj['__engaged_']:
            return obj['__val_'].address
        raise Exception('Trying to access data of empty std::optional')

class OptionalHasValueWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, element_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.element_type = element_type

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return gdb.lookup_type('bool')

    def __call__(self, obj):
        return obj['__engaged_']

class OptionalMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'std::optional')
        self.class_name = _versioned_namespace + 'optional'
        self.method_workers = {
            'value': OptionalValueWorker,
            'operator*': OptionalValueWorker,
            'operator->': OptionalValuePointerWorker,
            'operator bool': OptionalHasValueWorker,
            'has_value': OptionalHasValueWorker
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            value_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, value_type)


class AtomicLoadWorker(gdb.xmethod.XMethodWorker):
    def __init__(self, class_type, value_type):
        gdb.xmethod.XMethodWorker.__init__(self)
        self.class_type = class_type
        self.value_type = value_type

    def get_arg_types(self):
        return None

    def get_result_type(self):
        return self.value_type

    def __call__(self, obj):
        return obj['__a_']['__a_value']


class AtomicMatcher(gdb.xmethod.XMethodMatcher):
    def __init__(self):
        gdb.xmethod.XMethodMatcher.__init__(self, 'std::atomic')
        self.class_name = _versioned_namespace + 'atomic'
        self.method_workers = {
            'load': AtomicLoadWorker
        }
        self.methods = [gdb.xmethod.XMethod(key) for key, value in self.method_workers.items()]

    def match(self, class_type, method_name):
        if not class_type.tag.startswith(self.class_name + '<'):
            return None
        method = self.method_workers.get(method_name)
        if method is None:
            return None
        try:
            value_type = class_type.template_argument(0)
        except:
            return None
        return method(class_type, value_type)


def register_xmethods():
    gdb.xmethod.register_xmethod_matcher(None, VectorMatcher())
    gdb.xmethod.register_xmethod_matcher(None, ArrayMatcher())
    gdb.xmethod.register_xmethod_matcher(None, StringMatcher())
    gdb.xmethod.register_xmethod_matcher(None, UniquePtrMatcher())
    gdb.xmethod.register_xmethod_matcher(None, SharedPtrMatcher())
    gdb.xmethod.register_xmethod_matcher(None, DequeMatcher())
    gdb.xmethod.register_xmethod_matcher(None, OptionalMatcher())
    gdb.xmethod.register_xmethod_matcher(None, AtomicMatcher())
