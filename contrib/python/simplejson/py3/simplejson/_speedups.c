/* -*- mode: C; c-file-style: "python"; c-basic-offset: 4 -*- */
#include "Python.h"
#include "structmember.h"
#include <limits.h>  /* CHAR_BIT */

#if PY_MAJOR_VERSION >= 3
#define PyInt_FromSsize_t PyLong_FromSsize_t
#define PyInt_AsSsize_t PyLong_AsSsize_t
#define PyInt_Check(obj) 0
#define PyInt_CheckExact(obj) 0
#define JSON_UNICHR Py_UCS4
#define JSON_InternFromString PyUnicode_InternFromString
#define PyString_GET_SIZE PyUnicode_GET_LENGTH
#define JSON_StringCheck PyUnicode_Check
#define PY2_UNUSED
#if PY_VERSION_HEX >= 0x030C0000
/* PyUnicode_READY was deprecated in 3.10 and is a no-op since 3.12
 * (PEP 623). Skip calling it on modern Python to avoid the deprecation
 * warning and the eventual removal. */
#undef PyUnicode_READY
#define PyUnicode_READY(obj) 0
#endif
#else /* PY_MAJOR_VERSION >= 3 */
#define PY2_UNUSED UNUSED
#define JSON_StringCheck(obj) (PyString_Check(obj) || PyUnicode_Check(obj))
#define PyBytes_Check PyString_Check
#define PyUnicode_READY(obj) 0
#define PyUnicode_KIND(obj) (sizeof(Py_UNICODE))
#define PyUnicode_DATA(obj) ((void *)(PyUnicode_AS_UNICODE(obj)))
#define PyUnicode_READ(kind, data, index) ((JSON_UNICHR)((const Py_UNICODE *)(data))[(index)])
#define PyUnicode_GET_LENGTH PyUnicode_GET_SIZE
#define JSON_UNICHR Py_UNICODE
#define JSON_InternFromString PyString_InternFromString
#endif /* PY_MAJOR_VERSION < 3 */

#if PY_VERSION_HEX < 0x03090000
#if !defined(PyObject_CallNoArgs)
#define PyObject_CallNoArgs(callable) PyObject_CallFunctionObjArgs(callable, NULL)
#endif
#if !defined(PyObject_CallOneArg)
#define PyObject_CallOneArg(callable, arg) PyObject_CallFunctionObjArgs(callable, arg, NULL)
#endif
#endif /* PY_VERSION_HEX < 0x03090000 */

#if PY_VERSION_HEX < 0x02070000
#if !defined(PyOS_string_to_double)
#define PyOS_string_to_double json_PyOS_string_to_double
static double
json_PyOS_string_to_double(const char *s, char **endptr, PyObject *overflow_exception);
static double
json_PyOS_string_to_double(const char *s, char **endptr, PyObject *overflow_exception)
{
    double x;
    assert(endptr == NULL);
    assert(overflow_exception == NULL);
    PyFPE_START_PROTECT("json_PyOS_string_to_double", return -1.0;)
    x = PyOS_ascii_atof(s);
    PyFPE_END_PROTECT(x)
    return x;
}
#endif
#endif /* PY_VERSION_HEX < 0x02070000 */

#if PY_VERSION_HEX < 0x02060000
#if !defined(Py_TYPE)
#define Py_TYPE(ob)     (((PyObject*)(ob))->ob_type)
#endif
#if !defined(Py_SIZE)
#define Py_SIZE(ob)     (((PyVarObject*)(ob))->ob_size)
#endif
#if !defined(PyVarObject_HEAD_INIT)
#define PyVarObject_HEAD_INIT(type, size) PyObject_HEAD_INIT(type) size,
#endif
#endif /* PY_VERSION_HEX < 0x02060000 */

#ifdef __GNUC__
#define UNUSED __attribute__((__unused__))
#else
#define UNUSED
#endif

/* Py_T_OBJECT_EX is the stable public name added in Python 3.12 for the
 * member descriptor type that raises AttributeError when the underlying
 * slot is NULL. Pre-3.12 headers spell it T_OBJECT_EX (via the
 * internal-ish <structmember.h>), with identical semantics. Use the
 * stable name everywhere in this file and fall back to the legacy
 * spelling on older Pythons. The previous spelling was plain T_OBJECT,
 * which returned Py_None for NULL slots and is deprecated in 3.12+. */
#if !defined(Py_T_OBJECT_EX)
#  define Py_T_OBJECT_EX T_OBJECT_EX
#endif

/* Py_BEGIN_CRITICAL_SECTION was added in Python 3.13.
   On older versions, define as no-ops. */
#if PY_VERSION_HEX < 0x030d0000
#define Py_BEGIN_CRITICAL_SECTION(op)
#define Py_END_CRITICAL_SECTION()
#endif


#define DEFAULT_ENCODING "utf-8"

/* Unified module state.
   On Python 3.13+ this is stored per-module (PEP 489) so that each
   subinterpreter gets its own copy. On older Python versions a single
   static instance is shared by the whole process. Either way, code
   accesses it via get_speedups_state(module_ref) so that call sites
   look identical on all versions. */
typedef struct {
    PyObject *PyScannerType;
    PyObject *PyEncoderType;
    PyObject *JSON_Infinity;
    PyObject *JSON_NegInfinity;
    PyObject *JSON_NaN;
    PyObject *JSON_EmptyUnicode;
#if PY_MAJOR_VERSION < 3
    PyObject *JSON_EmptyStr;
    PyObject *JSON_EmptyStr_join;  /* bound method: ''.join */
#endif
    PyObject *JSON_s_null;
    PyObject *JSON_s_true;
    PyObject *JSON_s_false;
    PyObject *JSON_open_dict;
    PyObject *JSON_close_dict;
    PyObject *JSON_empty_dict;
    PyObject *JSON_open_array;
    PyObject *JSON_close_array;
    PyObject *JSON_empty_array;
    PyObject *JSON_newline;     /* "\n", prepended before each indent */
    PyObject *JSON_sortargs;
    PyObject *JSON_itemgetter0;
    /* Interned attribute-name strings used in hot paths. Caching them
     * here lets the scanner/encoder use PyObject_GetAttr (which takes
     * a PyObject *) instead of PyObject_GetAttrString (which interns
     * the C string every call). */
    PyObject *JSON_attr_for_json;     /* "for_json" */
    PyObject *JSON_attr_asdict;       /* "_asdict" */
    PyObject *JSON_attr_sort;         /* "sort" */
    PyObject *JSON_attr_encoded_json; /* "encoded_json" */
    PyObject *JSON_attr_add_note;     /* "add_note" (PEP 678, 3.11+) */
    PyObject *RawJSONType;
    PyObject *JSONDecodeError;
} _speedups_state;

#if PY_VERSION_HEX >= 0x030D0000
/* Forward declaration - defined later with multi-phase init */
static struct PyModuleDef moduledef;
#else
/* Pre-3.13: a single static state instance serves the whole process,
   and a borrowed reference to the module object so that Scanner and
   Encoder instances can store module_ref uniformly. The module object
   is kept alive by sys.modules for the entire interpreter lifetime.
   PyScannerType and PyEncoderType are defined later in this file
   (the full PyTypeObject bodies); their addresses are cached in
   _speedups_static_state.{PyScannerType,PyEncoderType} by
   module_exec, so the PyScanner_Check / PyEncoder_Check macros don't
   need a forward declaration of those symbols. */
static _speedups_state _speedups_static_state;
static PyObject *_speedups_module = NULL;  /* borrowed */
#define PyScanner_Check(op) \
    PyObject_TypeCheck(op, (PyTypeObject *)_speedups_static_state.PyScannerType)
#define PyScanner_CheckExact(op) \
    (Py_TYPE(op) == (PyTypeObject *)_speedups_static_state.PyScannerType)
#define PyEncoder_Check(op) \
    PyObject_TypeCheck(op, (PyTypeObject *)_speedups_static_state.PyEncoderType)
#define PyEncoder_CheckExact(op) \
    (Py_TYPE(op) == (PyTypeObject *)_speedups_static_state.PyEncoderType)
#endif

static inline _speedups_state *
get_speedups_state(PyObject *module)
{
    /* Every call site passes either the module object (from module-level
     * methods) or Scanner/Encoder->module_ref (set during instance
     * construction). Both must be non-NULL; catch any regression where an
     * uninitialized instance leaks into the hot path. */
    assert(module != NULL);
#if PY_VERSION_HEX >= 0x030D0000
    {
        /* Wrapped in an inner block so `state` is declared at the top
         * of a scope, keeping the file C89-clean under
         * -Wdeclaration-after-statement. */
        void *state = PyModule_GetState(module);
        assert(state != NULL);
        return (_speedups_state *)state;
    }
#else
    (void)module;
    return &_speedups_static_state;
#endif
}

#define JSON_ALLOW_NAN 1
#define JSON_IGNORE_NAN 2

#if PY_VERSION_HEX >= 0x030E0000
/* Python 3.14+: JSON_Accu is backed by a PyUnicodeWriter, building the
 * entire output in one contiguous buffer.  The FinishAsList wrapper
 * returns a single-element list so the Python caller's ''.join(chunks)
 * is effectively a no-op. */
typedef struct {
    PyUnicodeWriter *writer;
} JSON_Accu;
#else
typedef struct {
    PyObject *large_strings;  /* A list of previously accumulated large strings */
    PyObject *small_strings;  /* Pending small strings */
} JSON_Accu;
#endif

static int
JSON_Accu_Init(JSON_Accu *acc);
static int
JSON_Accu_Accumulate(_speedups_state *state, JSON_Accu *acc, PyObject *unicode);
static PyObject *
JSON_Accu_FinishAsList(_speedups_state *state, JSON_Accu *acc);
static void
JSON_Accu_Destroy(JSON_Accu *acc);

#define ERR_EXPECTING_VALUE "Expecting value"
#define ERR_ARRAY_DELIMITER "Expecting ',' delimiter or ']'"
#define ERR_ARRAY_VALUE_FIRST "Expecting value or ']'"
#define ERR_OBJECT_DELIMITER "Expecting ',' delimiter or '}'"
#define ERR_OBJECT_PROPERTY "Expecting property name enclosed in double quotes"
#define ERR_OBJECT_PROPERTY_FIRST "Expecting property name enclosed in double quotes or '}'"
#define ERR_OBJECT_PROPERTY_DELIMITER "Expecting ':' delimiter"
#define ERR_STRING_UNTERMINATED "Unterminated string starting at"
#define ERR_STRING_CONTROL "Invalid control character %r at"
#define ERR_STRING_ESC1 "Invalid \\X escape sequence %r"
#define ERR_STRING_ESC4 "Invalid \\uXXXX escape sequence"
#define ERR_TRAILING_COMMA_OBJECT "Illegal trailing comma before end of object"
#define ERR_TRAILING_COMMA_ARRAY "Illegal trailing comma before end of array"


typedef struct _PyScannerObject {
    PyObject_HEAD
    PyObject *module_ref;
    PyObject *encoding;
    PyObject *strict_bool;
    int strict;
    PyObject *object_hook;
    PyObject *pairs_hook;
    PyObject *array_hook;
    PyObject *parse_float;
    PyObject *parse_int;
    PyObject *parse_constant;
    PyObject *memo;
} PyScannerObject;

/* X-macro listing every PyObject* field in PyScannerObject that must
 * be visited by tp_traverse and released by tp_clear. Keep in sync with
 * the struct above; adding a new Py_Object* field here is sufficient to
 * make both scanner_traverse and scanner_clear handle it. Fields of
 * plain int/bool types (`strict`) don't participate in GC and are
 * intentionally omitted. */
#define JSON_SCANNER_OBJECT_FIELDS(X) \
    X(module_ref)                     \
    X(encoding)                       \
    X(strict_bool)                    \
    X(object_hook)                    \
    X(pairs_hook)                     \
    X(array_hook)                     \
    X(parse_float)                    \
    X(parse_int)                      \
    X(parse_constant)                 \
    X(memo)

static PyMemberDef scanner_members[] = {
    {"encoding", Py_T_OBJECT_EX, offsetof(PyScannerObject, encoding), READONLY, "encoding"},
    {"strict", Py_T_OBJECT_EX, offsetof(PyScannerObject, strict_bool), READONLY, "strict"},
    {"object_hook", Py_T_OBJECT_EX, offsetof(PyScannerObject, object_hook), READONLY, "object_hook"},
    {"object_pairs_hook", Py_T_OBJECT_EX, offsetof(PyScannerObject, pairs_hook), READONLY, "object_pairs_hook"},
    {"array_hook", Py_T_OBJECT_EX, offsetof(PyScannerObject, array_hook), READONLY, "array_hook"},
    {"parse_float", Py_T_OBJECT_EX, offsetof(PyScannerObject, parse_float), READONLY, "parse_float"},
    {"parse_int", Py_T_OBJECT_EX, offsetof(PyScannerObject, parse_int), READONLY, "parse_int"},
    {"parse_constant", Py_T_OBJECT_EX, offsetof(PyScannerObject, parse_constant), READONLY, "parse_constant"},
    {NULL}
};

typedef struct _PyEncoderObject {
    PyObject_HEAD
    PyObject *module_ref;
    PyObject *markers;
    PyObject *defaultfn;
    PyObject *encoder;
    PyObject *indent;
    PyObject *key_separator;
    PyObject *item_separator;
    PyObject *sort_keys;
    PyObject *key_memo;
    PyObject *encoding;
    PyObject *Decimal;
    PyObject *skipkeys_bool;
    int skipkeys;
    int fast_encode;
    /* 0, JSON_ALLOW_NAN, JSON_IGNORE_NAN */
    int allow_or_ignore_nan;
    int use_decimal;
    int namedtuple_as_object;
    int tuple_as_array;
    int iterable_as_array;
    PyObject *max_long_size;
    PyObject *min_long_size;
    PyObject *item_sort_key;
    PyObject *item_sort_kw;
    int for_json;
} PyEncoderObject;

/* X-macro listing every PyObject* field in PyEncoderObject that must
 * be visited by tp_traverse and released by tp_clear. See the comment
 * on JSON_SCANNER_OBJECT_FIELDS above. Int flag fields (skipkeys,
 * fast_encode, for_json, etc.) are omitted because they don't
 * participate in GC. */
#define JSON_ENCODER_OBJECT_FIELDS(X) \
    X(module_ref)                     \
    X(markers)                        \
    X(defaultfn)                      \
    X(encoder)                        \
    X(encoding)                       \
    X(indent)                         \
    X(key_separator)                  \
    X(item_separator)                 \
    X(key_memo)                       \
    X(skipkeys_bool)                  \
    X(sort_keys)                      \
    X(item_sort_kw)                   \
    X(item_sort_key)                  \
    X(max_long_size)                  \
    X(min_long_size)                  \
    X(Decimal)

static PyMemberDef encoder_members[] = {
    {"markers", Py_T_OBJECT_EX, offsetof(PyEncoderObject, markers), READONLY, "markers"},
    {"default", Py_T_OBJECT_EX, offsetof(PyEncoderObject, defaultfn), READONLY, "default"},
    {"encoder", Py_T_OBJECT_EX, offsetof(PyEncoderObject, encoder), READONLY, "encoder"},
    {"encoding", Py_T_OBJECT_EX, offsetof(PyEncoderObject, encoding), READONLY, "encoding"},
    {"indent", Py_T_OBJECT_EX, offsetof(PyEncoderObject, indent), READONLY, "indent"},
    {"key_separator", Py_T_OBJECT_EX, offsetof(PyEncoderObject, key_separator), READONLY, "key_separator"},
    {"item_separator", Py_T_OBJECT_EX, offsetof(PyEncoderObject, item_separator), READONLY, "item_separator"},
    {"sort_keys", Py_T_OBJECT_EX, offsetof(PyEncoderObject, sort_keys), READONLY, "sort_keys"},
    /* Python 2.5 does not support T_BOOl */
    {"skipkeys", Py_T_OBJECT_EX, offsetof(PyEncoderObject, skipkeys_bool), READONLY, "skipkeys"},
    {"key_memo", Py_T_OBJECT_EX, offsetof(PyEncoderObject, key_memo), READONLY, "key_memo"},
    {"item_sort_key", Py_T_OBJECT_EX, offsetof(PyEncoderObject, item_sort_key), READONLY, "item_sort_key"},
    {"max_long_size", Py_T_OBJECT_EX, offsetof(PyEncoderObject, max_long_size), READONLY, "max_long_size"},
    {"min_long_size", Py_T_OBJECT_EX, offsetof(PyEncoderObject, min_long_size), READONLY, "min_long_size"},
    {NULL}
};

#if PY_VERSION_HEX < 0x030E0000
static PyObject *
join_list_unicode(_speedups_state *state, PyObject *lst);
#endif
static PyObject *
JSON_ParseEncoding(PyObject *encoding);
static PyObject *
maybe_quote_bigint(PyEncoderObject* s, PyObject *encoded, PyObject *obj);
#if PY_VERSION_HEX < 0x030E0000 || PY_MAJOR_VERSION < 3
static Py_ssize_t
ascii_char_size(JSON_UNICHR c);
#endif
static Py_ssize_t
ascii_escape_char(JSON_UNICHR c, char *output, Py_ssize_t chars);
static PyObject *
ascii_escape_unicode(PyObject *pystr);
static PyObject *
ascii_escape_str(PyObject *pystr);
static PyObject *
py_encode_basestring_ascii(PyObject* self UNUSED, PyObject *pystr);
static PyObject *
py_encode_basestring(PyObject* self UNUSED, PyObject *pystr);
#if PY_MAJOR_VERSION < 3
static PyObject *
join_list_string(_speedups_state *state, PyObject *lst);
static PyObject *
scan_once_str(PyScannerObject *s, PyObject *pystr, Py_ssize_t idx, Py_ssize_t *next_idx_ptr);
static PyObject *
scanstring_str(_speedups_state *state, PyObject *pystr, Py_ssize_t end,
               const char *encoding, int strict, Py_ssize_t *next_end_ptr);
static PyObject *
_parse_object_str(PyScannerObject *s, PyObject *pystr, Py_ssize_t idx, Py_ssize_t *next_idx_ptr);
#endif
static PyObject *
scanstring_unicode(_speedups_state *state, PyObject *pystr, Py_ssize_t end,
                   int strict, Py_ssize_t *next_end_ptr);
static PyObject *
scan_once_unicode(PyScannerObject *s, PyObject *pystr, Py_ssize_t idx, Py_ssize_t *next_idx_ptr);
static PyObject *
_build_rval_index_tuple(PyObject *rval, Py_ssize_t idx)
{
    /* return (rval, idx) tuple, stealing reference to rval */
    if (rval == NULL) {
        assert(PyErr_Occurred());
        return NULL;
    }
    return Py_BuildValue("(Nn)", rval, idx);
}
static PyObject *
scanner_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
static void
scanner_dealloc(PyObject *self);
static int
scanner_clear(PyObject *self);
static PyObject *
encoder_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
static void
encoder_dealloc(PyObject *self);
static int
encoder_clear(PyObject *self);
static int
is_raw_json(_speedups_state *state, PyObject *obj);
static PyObject *
encoder_stringify_key(PyEncoderObject *s, PyObject *key);
static int
encoder_listencode_list(PyEncoderObject *s, JSON_Accu *rval, PyObject *seq, Py_ssize_t indent_level);
static int
encoder_listencode_obj(PyEncoderObject *s, JSON_Accu *rval, PyObject *obj, Py_ssize_t indent_level);
static int
encoder_listencode_dict(PyEncoderObject *s, JSON_Accu *rval, PyObject *dct, Py_ssize_t indent_level);
static PyObject *
_encoded_const(_speedups_state *state, PyObject *obj);
static void
raise_errmsg(_speedups_state *state, const char *msg, PyObject *s, Py_ssize_t end);
static PyObject *
encoder_encode_string(PyEncoderObject *s, PyObject *obj);
static int
_call_json_method(PyObject *obj, PyObject *method_name, PyObject **result);
static PyObject *
encoder_long_to_str(PyObject *obj);
static PyObject *
encoder_encode_float(PyEncoderObject *s, PyObject *obj);
static int
encoder_accumulate_newline_indent(PyEncoderObject *s, _speedups_state *state,
                                  JSON_Accu *rval, Py_ssize_t indent_level);
#if PY_VERSION_HEX >= 0x030B0000
static void
encoder_annotate_exception(_speedups_state *state, const char *format, ...);
#endif
static int
init_speedups_state(_speedups_state *state, PyObject *module);
static PyObject *
import_dependency(const char *module_name, const char *attr_name);

#define S_CHAR(c) (c >= ' ' && c <= '~' && c != '\\' && c != '"')
/* NON_ASCII_ESCAPE: character needs escaping in ensure_ascii=False mode.
 * Only control characters (0x00-0x1f), backslash and double-quote. */
#define NEEDS_ESCAPE(c) ((c) <= 0x1f || (c) == '\\' || (c) == '"')
#define IS_WHITESPACE(c) (((c) == ' ') || ((c) == '\t') || ((c) == '\n') || ((c) == '\r'))

#define MIN_EXPANSION 6

/* Cross-version helpers for dict ops used in the scanner/encoder hot
 * paths. On Python 3.13+ these forward to the new APIs that atomically
 * return strong references (avoiding borrowed-ref races under free
 * threading); on older Python versions they fall back to the legacy
 * borrowed-ref APIs with explicit Py_INCREF. */

static inline int
json_PyDict_GetItemRef(PyObject *dict, PyObject *key, PyObject **result)
{
    /* Atomically fetch a strong reference to dict[key]. Returns 1 if
     * found (with *result set to a new strong reference), 0 if not
     * found (with *result set to NULL), -1 on error. */
#if PY_VERSION_HEX >= 0x030D0000
    return PyDict_GetItemRef(dict, key, result);
#elif PY_VERSION_HEX >= 0x03040000
    /* PyDict_GetItemWithError was added in Python 3.4. */
    PyObject *obj = PyDict_GetItemWithError(dict, key);
    if (obj != NULL) {
        Py_INCREF(obj);
        *result = obj;
        return 1;
    }
    *result = NULL;
    return PyErr_Occurred() ? -1 : 0;
#else
    /* Python 2.7: PyDict_GetItem returns NULL without setting an
     * exception on missing keys and suppresses errors during lookup. */
    PyObject *obj = PyDict_GetItem(dict, key);  /* borrowed, no error */
    if (obj != NULL) {
        Py_INCREF(obj);
        *result = obj;
        return 1;
    }
    *result = NULL;
    return 0;
#endif
}

static inline int
json_memo_intern_key(PyObject *memo, PyObject **key_ptr)
{
    /* Intern *key_ptr into memo with a single atomic lookup, replacing
     * *key_ptr with a strong reference to the canonical entry (the
     * existing one if already present, or *key_ptr itself if it was
     * freshly inserted). The original reference in *key_ptr is always
     * dropped on success. Returns 0 on success, -1 on error. */
    PyObject *old = *key_ptr;
#if PY_VERSION_HEX >= 0x030D0000
    PyObject *canonical = NULL;
    if (PyDict_SetDefaultRef(memo, old, old, &canonical) < 0)
        return -1;
    Py_DECREF(old);
    *key_ptr = canonical;
    return 0;
#elif PY_VERSION_HEX >= 0x03040000
    /* PyDict_SetDefault was added in Python 3.4 and returns a borrowed
     * reference to the canonical entry. */
    PyObject *canonical = PyDict_SetDefault(memo, old, old);
    if (canonical == NULL)
        return -1;
    Py_INCREF(canonical);
    Py_DECREF(old);
    *key_ptr = canonical;
    return 0;
#else
    /* Python 2.7: no PyDict_SetDefault, use GetItem + SetItem. */
    PyObject *canonical = PyDict_GetItem(memo, old);  /* borrowed, no error */
    if (canonical == NULL) {
        if (PyDict_SetItem(memo, old, old) < 0)
            return -1;
        canonical = old;
    }
    Py_INCREF(canonical);
    Py_DECREF(old);
    *key_ptr = canonical;
    return 0;
#endif
}

/* Check if obj is a dict or frozendict (Python 3.15+).
 * PyAnyDict_Check is provided by CPython 3.15; on older versions
 * fall back to plain PyDict_Check. */
#if PY_VERSION_HEX >= 0x030F0000
#define JSON_AnyDict_Check(obj) PyAnyDict_Check(obj)
#else
#define JSON_AnyDict_Check(obj) PyDict_Check(obj)
#endif

static int
is_raw_json(_speedups_state *state, PyObject *obj)
{
    int r = PyObject_IsInstance(obj, state->RawJSONType);
    if (r < 0)
        return -1;
    return r;
}

#if PY_VERSION_HEX >= 0x030E0000
/* ---- PyUnicodeWriter-backed JSON_Accu (Python 3.14+) ---- */

static int
JSON_Accu_Init(JSON_Accu *acc)
{
    acc->writer = PyUnicodeWriter_Create(0);
    if (acc->writer == NULL)
        return -1;
    return 0;
}

static int
JSON_Accu_Accumulate(_speedups_state *state, JSON_Accu *acc, PyObject *unicode)
{
    (void)state;
    assert(PyUnicode_Check(unicode));
    return PyUnicodeWriter_WriteStr(acc->writer, unicode);
}

static PyObject *
JSON_Accu_FinishAsList(_speedups_state *state, JSON_Accu *acc)
{
    PyObject *str;
    PyObject *list;
    (void)state;
    str = PyUnicodeWriter_Finish(acc->writer);
    acc->writer = NULL;  /* Finish consumed the writer */
    if (str == NULL)
        return NULL;
    list = PyList_New(1);
    if (list == NULL) {
        Py_DECREF(str);
        return NULL;
    }
    PyList_SET_ITEM(list, 0, str);
    return list;
}

static void
JSON_Accu_Destroy(JSON_Accu *acc)
{
    if (acc->writer != NULL) {
        PyUnicodeWriter_Discard(acc->writer);
        acc->writer = NULL;
    }
}

#else /* PY_VERSION_HEX < 0x030E0000 */
/* ---- List-backed JSON_Accu (Python < 3.14) ---- */

static int
JSON_Accu_Init(JSON_Accu *acc)
{
    /* Lazily allocated */
    acc->large_strings = NULL;
    acc->small_strings = PyList_New(0);
    if (acc->small_strings == NULL)
        return -1;
    return 0;
}

static int
flush_accumulator(_speedups_state *state, JSON_Accu *acc)
{
    Py_ssize_t nsmall = PyList_GET_SIZE(acc->small_strings);
    if (nsmall) {
        int ret;
        PyObject *joined;
        if (acc->large_strings == NULL) {
            acc->large_strings = PyList_New(0);
            if (acc->large_strings == NULL)
                return -1;
        }
#if PY_MAJOR_VERSION >= 3
        joined = join_list_unicode(state, acc->small_strings);
#else
        joined = join_list_string(state, acc->small_strings);
#endif
        if (joined == NULL)
            return -1;
        if (PyList_SetSlice(acc->small_strings, 0, nsmall, NULL)) {
            Py_DECREF(joined);
            return -1;
        }
        ret = PyList_Append(acc->large_strings, joined);
        Py_DECREF(joined);
        return ret;
    }
    return 0;
}

static int
JSON_Accu_Accumulate(_speedups_state *state, JSON_Accu *acc, PyObject *unicode)
{
    Py_ssize_t nsmall;
#if PY_MAJOR_VERSION >= 3
    assert(PyUnicode_Check(unicode));
#else /* PY_MAJOR_VERSION >= 3 */
    assert(PyString_Check(unicode) || PyUnicode_Check(unicode));
#endif /* PY_MAJOR_VERSION < 3 */

    if (PyList_Append(acc->small_strings, unicode))
        return -1;
    nsmall = PyList_GET_SIZE(acc->small_strings);
    /* Each item in a list of unicode objects has an overhead (in 64-bit
     * builds) of:
     *   - 8 bytes for the list slot
     *   - 56 bytes for the header of the unicode object
     * that is, 64 bytes.  100000 such objects waste more than 6MB
     * compared to a single concatenated string.
     */
    if (nsmall < 100000)
        return 0;
    return flush_accumulator(state, acc);
}

static PyObject *
JSON_Accu_FinishAsList(_speedups_state *state, JSON_Accu *acc)
{
    int ret;
    PyObject *res;

    ret = flush_accumulator(state, acc);
    Py_CLEAR(acc->small_strings);
    if (ret) {
        Py_CLEAR(acc->large_strings);
        return NULL;
    }
    res = acc->large_strings;
    acc->large_strings = NULL;
    if (res == NULL)
        return PyList_New(0);
    return res;
}

static void
JSON_Accu_Destroy(JSON_Accu *acc)
{
    /* Safe to call unconditionally, including after JSON_Accu_FinishAsList
     * (which clears small_strings and transfers ownership of
     * large_strings to its return value). Py_CLEAR handles the NULL
     * case, so repeat calls are no-ops. */
    Py_CLEAR(acc->small_strings);
    Py_CLEAR(acc->large_strings);
}
#endif /* PY_VERSION_HEX >= 0x030E0000 */

static int
IS_DIGIT(JSON_UNICHR c)
{
    return c >= '0' && c <= '9';
}

static PyObject *
maybe_quote_bigint(PyEncoderObject* s, PyObject *encoded, PyObject *obj)
{
    int ge, le;
    PyObject *quoted;

    /* int_as_string_bitcount is not set: fast path, return as-is. */
    if (s->max_long_size == Py_None || s->min_long_size == Py_None)
        return encoded;

    ge = PyObject_RichCompareBool(obj, s->max_long_size, Py_GE);
    if (ge < 0) {
        Py_DECREF(encoded);
        return NULL;
    }
    le = PyObject_RichCompareBool(obj, s->min_long_size, Py_LE);
    if (le < 0) {
        Py_DECREF(encoded);
        return NULL;
    }
    if (!(ge || le))
        return encoded;

#if PY_MAJOR_VERSION >= 3
    quoted = PyUnicode_FromFormat("\"%U\"", encoded);
#else
    quoted = PyString_FromFormat("\"%s\"", PyString_AsString(encoded));
#endif
    Py_DECREF(encoded);
    return quoted;
}

/* Stringify an int/long to its JSON decimal form. For int/long subclasses
 * we first normalize through PyLong_Type so custom __str__ / __repr__
 * overrides don't inject garbage into the JSON output (see #118). */
static PyObject *
encoder_long_to_str(PyObject *obj)
{
    PyObject *encoded;
    PyObject *tmp;
    if (PyInt_CheckExact(obj) || PyLong_CheckExact(obj))
        return PyObject_Str(obj);
    tmp = PyObject_CallOneArg((PyObject *)&PyLong_Type, obj);
    if (tmp == NULL)
        return NULL;
    encoded = PyObject_Str(tmp);
    Py_DECREF(tmp);
    return encoded;
}

static int
_call_json_method(PyObject *obj, PyObject *method_name, PyObject **result)
{
    int rval = 0;
    /* method_name is an interned PyObject string cached in module state
     * (state->JSON_attr_for_json or state->JSON_attr_asdict), so this
     * avoids the char-to-interned-unicode conversion on every call. */
    PyObject *method = PyObject_GetAttr(obj, method_name);
    if (method == NULL) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            return 0;
        }
        /* Non-AttributeError from __getattr__ (e.g. MemoryError,
         * KeyboardInterrupt): propagate via NULL result so the caller
         * forwards the pending exception to encoder_steal_encode. */
        *result = NULL;
        return 1;
    }
    if (PyCallable_Check(method)) {
        PyObject *tmp = PyObject_CallNoArgs(method);
        if (tmp == NULL && PyErr_ExceptionMatches(PyExc_TypeError)) {
            PyErr_Clear();
        } else {
            /* This will set result to NULL if a TypeError occurred,
             * which must be checked by the caller */
            *result = tmp;
            rval = 1;
        }
    }
    Py_DECREF(method);
    return rval;
}

static Py_ssize_t
ascii_escape_char(JSON_UNICHR c, char *output, Py_ssize_t chars)
{
    /* Escape unicode code point c to ASCII escape sequences
    in char *output. output must have at least 12 bytes unused to
    accommodate an escaped surrogate pair "\uXXXX\uXXXX" */
    if (S_CHAR(c)) {
        output[chars++] = (char)c;
    }
    else {
        output[chars++] = '\\';
        switch (c) {
            case '\\': output[chars++] = (char)c; break;
            case '"': output[chars++] = (char)c; break;
            case '\b': output[chars++] = 'b'; break;
            case '\f': output[chars++] = 'f'; break;
            case '\n': output[chars++] = 'n'; break;
            case '\r': output[chars++] = 'r'; break;
            case '\t': output[chars++] = 't'; break;
            default:
#if PY_MAJOR_VERSION >= 3 || defined(Py_UNICODE_WIDE)
                if (c >= 0x10000) {
                    /* UTF-16 surrogate pair */
                    JSON_UNICHR v = c - 0x10000;
                    c = 0xd800 | ((v >> 10) & 0x3ff);
                    output[chars++] = 'u';
                    output[chars++] = "0123456789abcdef"[(c >> 12) & 0xf];
                    output[chars++] = "0123456789abcdef"[(c >>  8) & 0xf];
                    output[chars++] = "0123456789abcdef"[(c >>  4) & 0xf];
                    output[chars++] = "0123456789abcdef"[(c      ) & 0xf];
                    c = 0xdc00 | (v & 0x3ff);
                    output[chars++] = '\\';
                }
#endif
                output[chars++] = 'u';
                output[chars++] = "0123456789abcdef"[(c >> 12) & 0xf];
                output[chars++] = "0123456789abcdef"[(c >>  8) & 0xf];
                output[chars++] = "0123456789abcdef"[(c >>  4) & 0xf];
                output[chars++] = "0123456789abcdef"[(c      ) & 0xf];
        }
    }
    return chars;
}

#if PY_VERSION_HEX < 0x030E0000 || PY_MAJOR_VERSION < 3
/* Only needed by the two-pass ascii_escape_unicode (pre-3.14) and
 * ascii_escape_str (Python 2). The PyUnicodeWriter path on 3.14+
 * computes sizes implicitly, so this would be unused there. */
static Py_ssize_t
ascii_char_size(JSON_UNICHR c)
{
    if (S_CHAR(c)) {
        return 1;
    }
    else if (c == '\\' ||
               c == '"'  ||
               c == '\b' ||
               c == '\f' ||
               c == '\n' ||
               c == '\r' ||
               c == '\t') {
        return 2;
    }
#if PY_MAJOR_VERSION >= 3 || defined(Py_UNICODE_WIDE)
    else if (c >= 0x10000U) {
        return 2 * MIN_EXPANSION;
    }
#endif
    else {
        return MIN_EXPANSION;
    }
}
#endif /* PY_VERSION_HEX < 0x030E0000 || PY_MAJOR_VERSION < 3 */

#if PY_VERSION_HEX >= 0x030E0000
static PyObject *
ascii_escape_unicode(PyObject *pystr)
{
    /* Single-pass implementation using PyUnicodeWriter (Python 3.14+).
     * Writes runs of safe characters via WriteSubstring and escape
     * sequences via WriteUTF8 (all escape output is pure ASCII). */
    Py_ssize_t i;
    Py_ssize_t input_chars = PyUnicode_GET_LENGTH(pystr);
    int kind = PyUnicode_KIND(pystr);
    void *data = PyUnicode_DATA(pystr);
    Py_ssize_t run_start = 0;
    PyUnicodeWriter *writer = PyUnicodeWriter_Create(input_chars + 2);
    if (writer == NULL)
        return NULL;
    if (PyUnicodeWriter_WriteChar(writer, '"') < 0)
        goto bail;
    for (i = 0; i < input_chars; i++) {
        JSON_UNICHR c = PyUnicode_READ(kind, data, i);
        if (S_CHAR(c))
            continue;
        /* Flush run of safe characters */
        if (i > run_start) {
            if (PyUnicodeWriter_WriteSubstring(writer, pystr, run_start, i) < 0)
                goto bail;
        }
        /* Write escape sequence */
        {
            char buf[12];
            Py_ssize_t len = ascii_escape_char(c, buf, 0);
            if (PyUnicodeWriter_WriteUTF8(writer, buf, len) < 0)
                goto bail;
        }
        run_start = i + 1;
    }
    /* Flush remaining safe characters */
    if (i > run_start) {
        if (PyUnicodeWriter_WriteSubstring(writer, pystr, run_start, i) < 0)
            goto bail;
    }
    if (PyUnicodeWriter_WriteChar(writer, '"') < 0)
        goto bail;
    return PyUnicodeWriter_Finish(writer);
bail:
    PyUnicodeWriter_Discard(writer);
    return NULL;
}
#else /* PY_VERSION_HEX < 0x030E0000 */
static PyObject *
ascii_escape_unicode(PyObject *pystr)
{
    /* Two-pass implementation: calculate exact output size, then fill. */
    Py_ssize_t i;
    Py_ssize_t input_chars = PyUnicode_GET_LENGTH(pystr);
    Py_ssize_t output_size = 2;
    Py_ssize_t chars;
    PY2_UNUSED int kind = PyUnicode_KIND(pystr);
    void *data = PyUnicode_DATA(pystr);
    PyObject *rval;
    char *output;

    output_size = 2;
    for (i = 0; i < input_chars; i++) {
        Py_ssize_t charsize = ascii_char_size(PyUnicode_READ(kind, data, i));
        if (output_size > PY_SSIZE_T_MAX - charsize) {
            PyErr_SetString(PyExc_OverflowError, "string is too long to escape");
            return NULL;
        }
        output_size += charsize;
    }
#if PY_MAJOR_VERSION >= 3
    rval = PyUnicode_New(output_size, 127);
    if (rval == NULL) {
        return NULL;
    }
    assert(PyUnicode_KIND(rval) == PyUnicode_1BYTE_KIND);
    output = (char *)PyUnicode_DATA(rval);
#else
    rval = PyString_FromStringAndSize(NULL, output_size);
    if (rval == NULL) {
        return NULL;
    }
    output = PyString_AS_STRING(rval);
#endif
    chars = 0;
    output[chars++] = '"';
    for (i = 0; i < input_chars; i++) {
        chars = ascii_escape_char(PyUnicode_READ(kind, data, i), output, chars);
    }
    output[chars++] = '"';
    assert(chars == output_size);
    return rval;
}
#endif /* PY_VERSION_HEX >= 0x030E0000 */

#if PY_MAJOR_VERSION >= 3

static PyObject *
ascii_escape_str(PyObject *pystr)
{
    PyObject *rval;
    PyObject *input = PyUnicode_DecodeUTF8(PyBytes_AS_STRING(pystr), PyBytes_GET_SIZE(pystr), NULL);
    if (input == NULL)
        return NULL;
    rval = ascii_escape_unicode(input);
    Py_DECREF(input);
    return rval;
}

#else /* PY_MAJOR_VERSION >= 3 */

static PyObject *
ascii_escape_str(PyObject *pystr)
{
    /* Take a PyString pystr and return a new ASCII-only escaped PyString */
    Py_ssize_t i;
    Py_ssize_t input_chars;
    Py_ssize_t output_size;
    Py_ssize_t chars;
    PyObject *rval;
    char *output;
    char *input_str;

    input_chars = PyString_GET_SIZE(pystr);
    input_str = PyString_AS_STRING(pystr);
    output_size = 2;

    /* Fast path for a string that's already ASCII */
    for (i = 0; i < input_chars; i++) {
        JSON_UNICHR c = (JSON_UNICHR)input_str[i];
        if (c > 0x7f) {
            /* We hit a non-ASCII character, bail to unicode mode */
            PyObject *uni;
            uni = PyUnicode_DecodeUTF8(input_str, input_chars, "strict");
            if (uni == NULL) {
                return NULL;
            }
            rval = ascii_escape_unicode(uni);
            Py_DECREF(uni);
            return rval;
        }
        {
            Py_ssize_t charsize = ascii_char_size(c);
            if (output_size > PY_SSIZE_T_MAX - charsize) {
                PyErr_SetString(PyExc_OverflowError, "string is too long to escape");
                return NULL;
            }
            output_size += charsize;
        }
    }

    rval = PyString_FromStringAndSize(NULL, output_size);
    if (rval == NULL) {
        return NULL;
    }
    chars = 0;
    output = PyString_AS_STRING(rval);
    output[chars++] = '"';
    for (i = 0; i < input_chars; i++) {
        chars = ascii_escape_char((JSON_UNICHR)input_str[i], output, chars);
    }
    output[chars++] = '"';
    assert(chars == output_size);
    return rval;
}
#endif /* PY_MAJOR_VERSION < 3 */

static PyObject *
encoder_stringify_key(PyEncoderObject *s, PyObject *key)
{
    _speedups_state *state = get_speedups_state(s->module_ref);
    if (PyUnicode_Check(key)) {
        Py_INCREF(key);
        return key;
    }
#if PY_MAJOR_VERSION >= 3
    else if (PyBytes_Check(key) && s->encoding != Py_None) {
        const char *encoding = PyUnicode_AsUTF8(s->encoding);
        if (encoding == NULL)
            return NULL;
        return PyUnicode_Decode(
            PyBytes_AS_STRING(key),
            PyBytes_GET_SIZE(key),
            encoding,
            NULL);
    }
#else /* PY_MAJOR_VERSION >= 3 */
    else if (PyString_Check(key)) {
        Py_INCREF(key);
        return key;
    }
#endif /* PY_MAJOR_VERSION < 3 */
    else if (PyFloat_Check(key)) {
        return encoder_encode_float(s, key);
    }
    else if (key == Py_True || key == Py_False || key == Py_None) {
        /* This must come before the PyInt_Check because
           True and False are also 1 and 0.*/
        return _encoded_const(state, key);
    }
    else if (PyInt_Check(key) || PyLong_Check(key)) {
        return encoder_long_to_str(key);
    }
    else if (s->use_decimal && PyObject_TypeCheck(key, (PyTypeObject *)s->Decimal)) {
        return PyObject_Str(key);
    }
    if (s->skipkeys) {
        Py_INCREF(Py_None);
        return Py_None;
    }
    PyErr_Format(PyExc_TypeError,
                 "keys must be str, int, float, bool or None, "
                 "not %.100s", key->ob_type->tp_name);
    return NULL;
}

/* Call list.sort(**item_sort_kw) on `lst`. Returns 0 on success,
 * -1 on error. Factored out so the fast and slow paths of
 * encoder_dict_iteritems share one implementation. */
static int
encoder_sort_items_inplace(PyEncoderObject *s, PyObject *lst)
{
    _speedups_state *state = get_speedups_state(s->module_ref);
    PyObject *sortfun;
    PyObject *sortres;
    sortfun = PyObject_GetAttr(lst, state->JSON_attr_sort);
    if (sortfun == NULL)
        return -1;
    sortres = PyObject_Call(sortfun, state->JSON_sortargs, s->item_sort_kw);
    Py_DECREF(sortfun);
    if (sortres == NULL)
        return -1;
    Py_DECREF(sortres);
    return 0;
}

/* True iff `key` is a Python string type that can be used verbatim
 * as a JSON object key (PyUnicode on all versions, PyString also on
 * Python 2). */
static inline int
is_json_string_key(PyObject *key)
{
#if PY_MAJOR_VERSION < 3
    if (PyString_Check(key))
        return 1;
#endif
    return PyUnicode_Check(key);
}

static PyObject *
encoder_dict_iteritems(PyEncoderObject *s, PyObject *dct)
{
    PyObject *items;
    PyObject *iter = NULL;
    PyObject *lst = NULL;
    PyObject *item = NULL;
    PyObject *kstr = NULL;
    Py_ssize_t size;
    Py_ssize_t i;
    if (PyDict_CheckExact(dct))
        items = PyDict_Items(dct);
    else
        items = PyMapping_Items(dct);
    if (items == NULL)
        return NULL;

    /* Unsorted path: return iter(items) directly. */
    if (s->item_sort_kw == Py_None) {
        iter = PyObject_GetIter(items);
        Py_DECREF(items);
        return iter;
    }

    /* Sorted path. Fast sub-path: if every key is already a JSON-
     * compatible string, sort the items list in place and return
     * iter(items). No per-item tuple rebuild, no list alloc, no
     * stringify branch in the hot loop. This is the overwhelmingly
     * common case — JSON object keys are typically strings. Scan the
     * list once to establish it; on any non-string key fall through
     * to the general path below. */
    size = PyList_GET_SIZE(items);
    for (i = 0; i < size; i++) {
        PyObject *it = PyList_GET_ITEM(items, i);
        PyObject *key;
        if (!PyTuple_Check(it) || Py_SIZE(it) != 2) {
            PyErr_SetString(PyExc_ValueError, "items must return 2-tuples");
            Py_DECREF(items);
            return NULL;
        }
        key = PyTuple_GET_ITEM(it, 0);
        if (!is_json_string_key(key))
            break;
    }
    if (i == size) {
        if (encoder_sort_items_inplace(s, items) < 0) {
            Py_DECREF(items);
            return NULL;
        }
        iter = PyObject_GetIter(items);
        Py_DECREF(items);
        return iter;
    }

    /* Slow path: at least one key needs to be stringified before the
     * sort. Walk the items list from the first offending index `i`,
     * accumulating a new list with replacement tuples as needed. */
    iter = PyObject_GetIter(items);
    Py_DECREF(items);
    if (iter == NULL)
        return NULL;
    lst = PyList_New(0);
    if (lst == NULL)
        goto bail;
    while ((item = PyIter_Next(iter))) {
        PyObject *key, *value;
        /* items comes from the original iter we built; the tuple shape
         * was already validated by the fast-path pre-scan above, but
         * a user-defined mapping could return non-tuples out of order
         * here if the 2-tuple check above happened to succeed on
         * other entries — revalidate. */
        if (!PyTuple_Check(item) || Py_SIZE(item) != 2) {
            PyErr_SetString(PyExc_ValueError, "items must return 2-tuples");
            goto bail;
        }
        key = PyTuple_GET_ITEM(item, 0);
        if (!is_json_string_key(key)) {
            PyObject *tpl;
            kstr = encoder_stringify_key(s, key);
            if (kstr == NULL)
                goto bail;
            if (kstr == Py_None) {
                /* skipkeys */
                Py_CLEAR(kstr);
                Py_CLEAR(item);
                continue;
            }
            value = PyTuple_GET_ITEM(item, 1);
            tpl = PyTuple_Pack(2, kstr, value);
            if (tpl == NULL)
                goto bail;
            Py_CLEAR(kstr);
            Py_DECREF(item);
            item = tpl;
        }
        if (PyList_Append(lst, item))
            goto bail;
        Py_CLEAR(item);
    }
    Py_CLEAR(iter);
    if (PyErr_Occurred())
        goto bail;
    if (encoder_sort_items_inplace(s, lst) < 0)
        goto bail;
    iter = PyObject_GetIter(lst);
    Py_CLEAR(lst);
    return iter;
bail:
    Py_XDECREF(kstr);
    Py_XDECREF(item);
    Py_XDECREF(lst);
    Py_XDECREF(iter);
    return NULL;
}

/* Use JSONDecodeError exception to raise a nice looking ValueError subclass */
static void
raise_errmsg(_speedups_state *state, const char *msg, PyObject *s, Py_ssize_t end)
{
    PyObject *JSONDecodeError = state->JSONDecodeError;
    PyObject *exc = PyObject_CallFunction(JSONDecodeError, "(zOn)", msg, s, end);
    if (exc) {
        PyErr_SetObject(JSONDecodeError, exc);
        Py_DECREF(exc);
    }
}

#if PY_VERSION_HEX < 0x030E0000
static PyObject *
join_list_unicode(_speedups_state *state, PyObject *lst)
{
    /* return u''.join(lst) */
    return PyUnicode_Join(state->JSON_EmptyUnicode, lst);
}
#endif

#if PY_MAJOR_VERSION < 3
static PyObject *
join_list_string(_speedups_state *state, PyObject *lst)
{
    /* return ''.join(lst) */
    return PyObject_CallOneArg(state->JSON_EmptyStr_join, lst);
}
#endif /* PY_MAJOR_VERSION < 3 */

#define APPEND_OLD_CHUNK \
    if (chunk != NULL) { \
        if (chunks == NULL) { \
            chunks = PyList_New(0); \
            if (chunks == NULL) { \
                goto bail; \
            } \
        } \
        if (PyList_Append(chunks, chunk)) { \
            goto bail; \
        } \
        Py_CLEAR(chunk); \
    }

#if PY_MAJOR_VERSION < 3
static PyObject *
scanstring_str(_speedups_state *state, PyObject *pystr, Py_ssize_t end,
               const char *encoding, int strict, Py_ssize_t *next_end_ptr)
{
    /* Read the JSON string from PyString pystr.
    end is the index of the first character after the quote.
    encoding is the encoding of pystr (must be an ASCII superset)
    if strict is zero then literal control characters are allowed
    *next_end_ptr is a return-by-reference index of the character
        after the end quote

    Return value is a new PyString (if ASCII-only) or PyUnicode
    */
    PyObject *rval;
    Py_ssize_t len = PyString_GET_SIZE(pystr);
    Py_ssize_t begin = end - 1;
    Py_ssize_t next = begin;
    int has_unicode = 0;
    char *buf = PyString_AS_STRING(pystr);
    PyObject *chunks = NULL;
    PyObject *chunk = NULL;
    PyObject *strchunk = NULL;

    if (len == end) {
        raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
        goto bail;
    }
    else if (end < 0 || len < end) {
        /* Out-of-range end: match py_scanstring, which raises
         * JSONDecodeError("Unterminated string starting at") so that
         * user code using `except JSONDecodeError` catches the C path
         * the same way it catches the pure-Python path. */
        raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
        goto bail;
    }
    while (1) {
        /* Find the end of the string or the next escape */
        Py_UNICODE c = 0;
        for (next = end; next < len; next++) {
            c = (unsigned char)buf[next];
            if (c == '"' || c == '\\') {
                break;
            }
            else if (strict && c <= 0x1f) {
                raise_errmsg(state, ERR_STRING_CONTROL, pystr, next);
                goto bail;
            }
            else if (c > 0x7f) {
                has_unicode = 1;
            }
        }
        if (!(c == '"' || c == '\\')) {
            raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
            goto bail;
        }
        /* Pick up this chunk if it's not zero length */
        if (next != end) {
            APPEND_OLD_CHUNK
            strchunk = PyString_FromStringAndSize(&buf[end], next - end);
            if (strchunk == NULL) {
                goto bail;
            }
            if (has_unicode) {
                chunk = PyUnicode_FromEncodedObject(strchunk, encoding, NULL);
                Py_DECREF(strchunk);
                if (chunk == NULL) {
                    goto bail;
                }
            }
            else {
                chunk = strchunk;
            }
        }
        next++;
        if (c == '"') {
            end = next;
            break;
        }
        if (next == len) {
            raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
            goto bail;
        }
        c = buf[next];
        if (c != 'u') {
            /* Non-unicode backslash escapes */
            end = next + 1;
            switch (c) {
                case '"': break;
                case '\\': break;
                case '/': break;
                case 'b': c = '\b'; break;
                case 'f': c = '\f'; break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                default: c = 0;
            }
            if (c == 0) {
                raise_errmsg(state, ERR_STRING_ESC1, pystr, end - 2);
                goto bail;
            }
        }
        else {
            c = 0;
            next++;
            end = next + 4;
            if (end > len) {
                raise_errmsg(state, ERR_STRING_ESC4, pystr, next - 2);
                goto bail;
            }
            /* Decode 4 hex digits */
            for (; next < end; next++) {
                JSON_UNICHR hex_digit = (JSON_UNICHR)buf[next];
                c <<= 4;
                switch (hex_digit) {
                    case '0': case '1': case '2': case '3': case '4':
                    case '5': case '6': case '7': case '8': case '9':
                        c |= (hex_digit - '0'); break;
                    case 'a': case 'b': case 'c': case 'd': case 'e':
                    case 'f':
                        c |= (hex_digit - 'a' + 10); break;
                    case 'A': case 'B': case 'C': case 'D': case 'E':
                    case 'F':
                        c |= (hex_digit - 'A' + 10); break;
                    default:
                        raise_errmsg(state, ERR_STRING_ESC4, pystr, end - 6);
                        goto bail;
                }
            }
#if defined(Py_UNICODE_WIDE)
            /* Surrogate pair */
            if ((c & 0xfc00) == 0xd800) {
                if (end + 6 <= len && buf[next] == '\\' && buf[next+1] == 'u') {
                    JSON_UNICHR c2 = 0;
                    end += 6;
                    /* Decode 4 hex digits */
                    for (next += 2; next < end; next++) {
                        c2 <<= 4;
                        JSON_UNICHR hex_digit = buf[next];
                        switch (hex_digit) {
                        case '0': case '1': case '2': case '3': case '4':
                        case '5': case '6': case '7': case '8': case '9':
                            c2 |= (hex_digit - '0'); break;
                        case 'a': case 'b': case 'c': case 'd': case 'e':
                        case 'f':
                            c2 |= (hex_digit - 'a' + 10); break;
                        case 'A': case 'B': case 'C': case 'D': case 'E':
                        case 'F':
                            c2 |= (hex_digit - 'A' + 10); break;
                        default:
                            raise_errmsg(state, ERR_STRING_ESC4, pystr, end - 6);
                            goto bail;
                        }
                    }
                    if ((c2 & 0xfc00) != 0xdc00) {
                        /* not a low surrogate, rewind */
                        end -= 6;
                        next = end;
                    }
                    else {
                        c = 0x10000 + (((c - 0xd800) << 10) | (c2 - 0xdc00));
                    }
                }
            }
#endif /* Py_UNICODE_WIDE */
        }
        if (c > 0x7f) {
            has_unicode = 1;
        }
        APPEND_OLD_CHUNK
        if (has_unicode) {
            chunk = PyUnicode_FromOrdinal(c);
            if (chunk == NULL) {
                goto bail;
            }
        }
        else {
            char c_char = Py_CHARMASK(c);
            chunk = PyString_FromStringAndSize(&c_char, 1);
            if (chunk == NULL) {
                goto bail;
            }
        }
    }

    if (chunks == NULL) {
        if (chunk != NULL)
            rval = chunk;
        else {
            rval = state->JSON_EmptyStr;
            Py_INCREF(rval);
        }
    }
    else {
        APPEND_OLD_CHUNK
        rval = join_list_string(state, chunks);
        if (rval == NULL) {
            goto bail;
        }
        Py_CLEAR(chunks);
    }

    *next_end_ptr = end;
    return rval;
bail:
    *next_end_ptr = -1;
    Py_XDECREF(chunk);
    Py_XDECREF(chunks);
    return NULL;
}
#endif /* PY_MAJOR_VERSION < 3 */

#if PY_VERSION_HEX >= 0x030E0000
static PyObject *
scanstring_unicode(_speedups_state *state, PyObject *pystr, Py_ssize_t end,
                   int strict, Py_ssize_t *next_end_ptr)
{
    /* Python 3.14+: use PyUnicodeWriter instead of a chunks list.
     * The writer is lazily created on the first escape sequence so that
     * the common no-escape path returns a cheap PyUnicode_Substring. */
    PyObject *rval;
    Py_ssize_t begin = end - 1;
    Py_ssize_t next = begin;
    int kind = PyUnicode_KIND(pystr);
    Py_ssize_t len = PyUnicode_GET_LENGTH(pystr);
    void *buf = PyUnicode_DATA(pystr);
    PyUnicodeWriter *writer = NULL;
    Py_ssize_t literal_start;

    if (len == end) {
        raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
        goto bail;
    }
    else if (end < 0 || len < end) {
        /* Out-of-range end: match py_scanstring, which raises
         * JSONDecodeError("Unterminated string starting at") so that
         * user code using `except JSONDecodeError` catches the C path
         * the same way it catches the pure-Python path. */
        raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
        goto bail;
    }

    literal_start = end;
    while (1) {
        /* Find the end of the string or the next escape */
        JSON_UNICHR c = 0;
        for (next = end; next < len; next++) {
            c = PyUnicode_READ(kind, buf, next);
            if (c == '"' || c == '\\') {
                break;
            }
            else if (strict && c <= 0x1f) {
                raise_errmsg(state, ERR_STRING_CONTROL, pystr, next);
                goto bail;
            }
        }
        if (!(c == '"' || c == '\\')) {
            raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
            goto bail;
        }
        next++;
        if (c == '"') {
            end = next;
            break;
        }
        /* Backslash escape — ensure writer exists and flush the
         * literal span [literal_start, next-1). */
        if (writer == NULL) {
            writer = PyUnicodeWriter_Create(len - begin);
            if (writer == NULL)
                goto bail;
        }
        if (next - 1 > literal_start) {
            if (PyUnicodeWriter_WriteSubstring(writer, pystr,
                                               literal_start, next - 1) < 0)
                goto bail;
        }
        if (next == len) {
            raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
            goto bail;
        }
        c = PyUnicode_READ(kind, buf, next);
        if (c != 'u') {
            /* Non-unicode backslash escapes */
            end = next + 1;
            switch (c) {
                case '"': break;
                case '\\': break;
                case '/': break;
                case 'b': c = '\b'; break;
                case 'f': c = '\f'; break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                default: c = 0;
            }
            if (c == 0) {
                raise_errmsg(state, ERR_STRING_ESC1, pystr, end - 2);
                goto bail;
            }
        }
        else {
            c = 0;
            next++;
            end = next + 4;
            if (end > len) {
                raise_errmsg(state, ERR_STRING_ESC4, pystr, next - 2);
                goto bail;
            }
            /* Decode 4 hex digits */
            for (; next < end; next++) {
                JSON_UNICHR hex_digit = PyUnicode_READ(kind, buf, next);
                c <<= 4;
                switch (hex_digit) {
                    case '0': case '1': case '2': case '3': case '4':
                    case '5': case '6': case '7': case '8': case '9':
                        c |= (hex_digit - '0'); break;
                    case 'a': case 'b': case 'c': case 'd': case 'e':
                    case 'f':
                        c |= (hex_digit - 'a' + 10); break;
                    case 'A': case 'B': case 'C': case 'D': case 'E':
                    case 'F':
                        c |= (hex_digit - 'A' + 10); break;
                    default:
                        raise_errmsg(state, ERR_STRING_ESC4, pystr, end - 6);
                        goto bail;
                }
            }
            /* Surrogate pair */
            if ((c & 0xfc00) == 0xd800) {
                JSON_UNICHR c2 = 0;
                if (end + 6 <= len &&
                    PyUnicode_READ(kind, buf, next) == '\\' &&
                    PyUnicode_READ(kind, buf, next + 1) == 'u') {
                    end += 6;
                    /* Decode 4 hex digits */
                    for (next += 2; next < end; next++) {
                        JSON_UNICHR hex_digit = PyUnicode_READ(kind, buf, next);
                        c2 <<= 4;
                        switch (hex_digit) {
                        case '0': case '1': case '2': case '3': case '4':
                        case '5': case '6': case '7': case '8': case '9':
                            c2 |= (hex_digit - '0'); break;
                        case 'a': case 'b': case 'c': case 'd': case 'e':
                        case 'f':
                            c2 |= (hex_digit - 'a' + 10); break;
                        case 'A': case 'B': case 'C': case 'D': case 'E':
                        case 'F':
                            c2 |= (hex_digit - 'A' + 10); break;
                        default:
                            raise_errmsg(state, ERR_STRING_ESC4, pystr, end - 6);
                            goto bail;
                        }
                    }
                    if ((c2 & 0xfc00) != 0xdc00) {
                        /* not a low surrogate, rewind */
                        end -= 6;
                        next = end;
                    }
                    else {
                        c = 0x10000 + (((c - 0xd800) << 10) | (c2 - 0xdc00));
                    }
                }
            }
        }
        if (PyUnicodeWriter_WriteChar(writer, c) < 0)
            goto bail;
        literal_start = end;
    }

    /* Finalize */
    if (writer == NULL) {
        /* No escape sequences: return a substring directly. */
        if (end - 1 > literal_start)
            rval = PyUnicode_Substring(pystr, literal_start, end - 1);
        else {
            rval = state->JSON_EmptyUnicode;
            Py_INCREF(rval);
        }
    }
    else {
        /* Flush trailing literal span after the last escape. */
        if (end - 1 > literal_start) {
            if (PyUnicodeWriter_WriteSubstring(writer, pystr,
                                               literal_start, end - 1) < 0)
                goto bail;
        }
        rval = PyUnicodeWriter_Finish(writer);
        writer = NULL;  /* Finish consumed the writer */
        if (rval == NULL)
            goto bail;
    }
    *next_end_ptr = end;
    return rval;
bail:
    if (writer != NULL)
        PyUnicodeWriter_Discard(writer);
    *next_end_ptr = -1;
    return NULL;
}
#else /* PY_VERSION_HEX < 0x030E0000 */
static PyObject *
scanstring_unicode(_speedups_state *state, PyObject *pystr, Py_ssize_t end,
                   int strict, Py_ssize_t *next_end_ptr)
{
    /* Read the JSON string from PyUnicode pystr.
    end is the index of the first character after the quote.
    if strict is zero then literal control characters are allowed
    *next_end_ptr is a return-by-reference index of the character
        after the end quote

    Return value is a new PyUnicode
    */
    PyObject *rval;
    Py_ssize_t begin = end - 1;
    Py_ssize_t next = begin;
    PY2_UNUSED int kind = PyUnicode_KIND(pystr);
    Py_ssize_t len = PyUnicode_GET_LENGTH(pystr);
    void *buf = PyUnicode_DATA(pystr);
    PyObject *chunks = NULL;
    PyObject *chunk = NULL;

    if (len == end) {
        raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
        goto bail;
    }
    else if (end < 0 || len < end) {
        /* Out-of-range end: match py_scanstring, which raises
         * JSONDecodeError("Unterminated string starting at") so that
         * user code using `except JSONDecodeError` catches the C path
         * the same way it catches the pure-Python path. */
        raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
        goto bail;
    }
    while (1) {
        /* Find the end of the string or the next escape */
        JSON_UNICHR c = 0;
        for (next = end; next < len; next++) {
            c = PyUnicode_READ(kind, buf, next);
            if (c == '"' || c == '\\') {
                break;
            }
            else if (strict && c <= 0x1f) {
                raise_errmsg(state, ERR_STRING_CONTROL, pystr, next);
                goto bail;
            }
        }
        if (!(c == '"' || c == '\\')) {
            raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
            goto bail;
        }
        /* Pick up this chunk if it's not zero length */
        if (next != end) {
            APPEND_OLD_CHUNK
#if PY_MAJOR_VERSION < 3
            chunk = PyUnicode_FromUnicode(&((const Py_UNICODE *)buf)[end], next - end);
#else
            chunk = PyUnicode_Substring(pystr, end, next);
#endif
            if (chunk == NULL) {
                goto bail;
            }
        }
        next++;
        if (c == '"') {
            end = next;
            break;
        }
        if (next == len) {
            raise_errmsg(state, ERR_STRING_UNTERMINATED, pystr, begin);
            goto bail;
        }
        c = PyUnicode_READ(kind, buf, next);
        if (c != 'u') {
            /* Non-unicode backslash escapes */
            end = next + 1;
            switch (c) {
                case '"': break;
                case '\\': break;
                case '/': break;
                case 'b': c = '\b'; break;
                case 'f': c = '\f'; break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                default: c = 0;
            }
            if (c == 0) {
                raise_errmsg(state, ERR_STRING_ESC1, pystr, end - 2);
                goto bail;
            }
        }
        else {
            c = 0;
            next++;
            end = next + 4;
            if (end > len) {
                raise_errmsg(state, ERR_STRING_ESC4, pystr, next - 2);
                goto bail;
            }
            /* Decode 4 hex digits */
            for (; next < end; next++) {
                JSON_UNICHR hex_digit = PyUnicode_READ(kind, buf, next);
                c <<= 4;
                switch (hex_digit) {
                    case '0': case '1': case '2': case '3': case '4':
                    case '5': case '6': case '7': case '8': case '9':
                        c |= (hex_digit - '0'); break;
                    case 'a': case 'b': case 'c': case 'd': case 'e':
                    case 'f':
                        c |= (hex_digit - 'a' + 10); break;
                    case 'A': case 'B': case 'C': case 'D': case 'E':
                    case 'F':
                        c |= (hex_digit - 'A' + 10); break;
                    default:
                        raise_errmsg(state, ERR_STRING_ESC4, pystr, end - 6);
                        goto bail;
                }
            }
#if PY_MAJOR_VERSION >= 3 || defined(Py_UNICODE_WIDE)
            /* Surrogate pair */
            if ((c & 0xfc00) == 0xd800) {
                JSON_UNICHR c2 = 0;
                if (end + 6 <= len &&
                    PyUnicode_READ(kind, buf, next) == '\\' &&
                    PyUnicode_READ(kind, buf, next + 1) == 'u') {
                    end += 6;
                    /* Decode 4 hex digits */
                    for (next += 2; next < end; next++) {
                        JSON_UNICHR hex_digit = PyUnicode_READ(kind, buf, next);
                        c2 <<= 4;
                        switch (hex_digit) {
                        case '0': case '1': case '2': case '3': case '4':
                        case '5': case '6': case '7': case '8': case '9':
                            c2 |= (hex_digit - '0'); break;
                        case 'a': case 'b': case 'c': case 'd': case 'e':
                        case 'f':
                            c2 |= (hex_digit - 'a' + 10); break;
                        case 'A': case 'B': case 'C': case 'D': case 'E':
                        case 'F':
                            c2 |= (hex_digit - 'A' + 10); break;
                        default:
                            raise_errmsg(state, ERR_STRING_ESC4, pystr, end - 6);
                            goto bail;
                        }
                    }
                    if ((c2 & 0xfc00) != 0xdc00) {
                        /* not a low surrogate, rewind */
                        end -= 6;
                        next = end;
                    }
                    else {
                        c = 0x10000 + (((c - 0xd800) << 10) | (c2 - 0xdc00));
                    }
                }
            }
#endif
        }
        APPEND_OLD_CHUNK
        chunk = PyUnicode_FromOrdinal(c);
        if (chunk == NULL) {
            goto bail;
        }
    }

    if (chunks == NULL) {
        if (chunk != NULL)
            rval = chunk;
        else {
            rval = state->JSON_EmptyUnicode;
            Py_INCREF(rval);
        }
    }
    else {
        APPEND_OLD_CHUNK
        rval = join_list_unicode(state, chunks);
        if (rval == NULL) {
            goto bail;
        }
        Py_CLEAR(chunks);
    }
    *next_end_ptr = end;
    return rval;
bail:
    *next_end_ptr = -1;
    Py_XDECREF(chunk);
    Py_XDECREF(chunks);
    return NULL;
}
#endif /* PY_VERSION_HEX >= 0x030E0000 */

PyDoc_STRVAR(pydoc_scanstring,
    "scanstring(basestring, end, encoding, strict=True) -> (str, end)\n"
    "\n"
    "Scan the string s for a JSON string. End is the index of the\n"
    "character in s after the quote that started the JSON string.\n"
    "Unescapes all valid JSON string escape sequences and raises ValueError\n"
    "on attempt to decode an invalid string. If strict is False then literal\n"
    "control characters are allowed in the string.\n"
    "\n"
    "Returns a tuple of the decoded string and the index of the character in s\n"
    "after the end quote."
);

static PyObject *
py_scanstring(PyObject* self UNUSED, PyObject *args)
{
    PyObject *pystr;
    PyObject *rval;
    Py_ssize_t end;
    Py_ssize_t next_end = -1;
    char *encoding = NULL;
    int strict = 1;
    if (!PyArg_ParseTuple(args, "On|zi:scanstring", &pystr, &end, &encoding, &strict)) {
        return NULL;
    }
    if (encoding == NULL) {
        encoding = DEFAULT_ENCODING;
    }
    if (PyUnicode_Check(pystr)) {
        if (PyUnicode_READY(pystr))
            return NULL;
        rval = scanstring_unicode(get_speedups_state(self), pystr, end,
                                  strict, &next_end);
    }
#if PY_MAJOR_VERSION < 3
    /* Using a bytes input is unsupported for scanning in Python 3.
       It is coerced to str in the decoder before it gets here. */
    else if (PyString_Check(pystr)) {
        rval = scanstring_str(get_speedups_state(self), pystr, end,
                              encoding, strict, &next_end);
    }
#endif
    else {
        PyErr_Format(PyExc_TypeError,
                     "first argument must be a string, not %.80s",
                     Py_TYPE(pystr)->tp_name);
        return NULL;
    }
    return _build_rval_index_tuple(rval, next_end);
}

PyDoc_STRVAR(pydoc_encode_basestring_ascii,
    "encode_basestring_ascii(basestring) -> str\n"
    "\n"
    "Return an ASCII-only JSON representation of a Python string"
);

PyDoc_STRVAR(pydoc_encode_basestring,
    "encode_basestring(basestring) -> str\n"
    "\n"
    "Return a JSON representation of a Python string"
);

static PyObject *
py_encode_basestring_ascii(PyObject* self UNUSED, PyObject *pystr)
{
    /* Return an ASCII-only JSON representation of a Python string */
    /* METH_O */
    if (PyBytes_Check(pystr)) {
        return ascii_escape_str(pystr);
    }
    else if (PyUnicode_Check(pystr)) {
        if (PyUnicode_READY(pystr))
            return NULL;
        return ascii_escape_unicode(pystr);
    }
    else {
        PyErr_Format(PyExc_TypeError,
                     "first argument must be a string, not %.80s",
                     Py_TYPE(pystr)->tp_name);
        return NULL;
    }
}

/* encode_basestring: escape only control chars, backslash, and quote.
 * Non-ASCII characters pass through unchanged (ensure_ascii=False). */

#if PY_VERSION_HEX < 0x030E0000 || PY_MAJOR_VERSION < 3
/* Only needed by the two-pass escape_unicode_noascii (pre-3.14 and
 * Python 2). The PyUnicodeWriter path on 3.14+ writes escapes inline. */
static Py_ssize_t
escape_char_noascii(JSON_UNICHR c, JSON_UNICHR *output, Py_ssize_t chars)
{
    if (!NEEDS_ESCAPE(c)) {
        output[chars++] = c;
    }
    else {
        output[chars++] = '\\';
        switch (c) {
            case '\\': output[chars++] = '\\'; break;
            case '"':  output[chars++] = '"'; break;
            case '\b': output[chars++] = 'b'; break;
            case '\f': output[chars++] = 'f'; break;
            case '\n': output[chars++] = 'n'; break;
            case '\r': output[chars++] = 'r'; break;
            case '\t': output[chars++] = 't'; break;
            default:
                /* Control character: \u00XX */
                output[chars++] = 'u';
                output[chars++] = '0';
                output[chars++] = '0';
                output[chars++] = "0123456789abcdef"[(c >> 4) & 0xf];
                output[chars++] = "0123456789abcdef"[(c     ) & 0xf];
        }
    }
    return chars;
}

static Py_ssize_t
escape_char_noascii_size(JSON_UNICHR c)
{
    if (!NEEDS_ESCAPE(c))
        return 1;
    switch (c) {
        case '\\': case '"': case '\b': case '\f':
        case '\n': case '\r': case '\t':
            return 2;
        default:
            return 6;  /* \u00XX */
    }
}
#endif /* PY_VERSION_HEX < 0x030E0000 || PY_MAJOR_VERSION < 3 */

#if PY_VERSION_HEX >= 0x030E0000
static PyObject *
escape_unicode_noascii(PyObject *pystr)
{
    /* Single-pass using PyUnicodeWriter (Python 3.14+). */
    Py_ssize_t i;
    Py_ssize_t input_chars = PyUnicode_GET_LENGTH(pystr);
    int kind = PyUnicode_KIND(pystr);
    void *data = PyUnicode_DATA(pystr);
    Py_ssize_t run_start = 0;
    PyUnicodeWriter *writer = PyUnicodeWriter_Create(input_chars + 2);
    if (writer == NULL)
        return NULL;
    if (PyUnicodeWriter_WriteChar(writer, '"') < 0)
        goto bail;
    for (i = 0; i < input_chars; i++) {
        JSON_UNICHR c = PyUnicode_READ(kind, data, i);
        if (!NEEDS_ESCAPE(c))
            continue;
        /* Flush run of safe characters */
        if (i > run_start) {
            if (PyUnicodeWriter_WriteSubstring(writer, pystr, run_start, i) < 0)
                goto bail;
        }
        {
            char buf[6]; /* longest escape: \u00XX */
            Py_ssize_t len = 0;
            buf[len++] = '\\';
            switch (c) {
                case '\\': buf[len++] = '\\'; break;
                case '"':  buf[len++] = '"'; break;
                case '\b': buf[len++] = 'b'; break;
                case '\f': buf[len++] = 'f'; break;
                case '\n': buf[len++] = 'n'; break;
                case '\r': buf[len++] = 'r'; break;
                case '\t': buf[len++] = 't'; break;
                default:
                    buf[len++] = 'u';
                    buf[len++] = '0';
                    buf[len++] = '0';
                    buf[len++] = "0123456789abcdef"[(c >> 4) & 0xf];
                    buf[len++] = "0123456789abcdef"[(c     ) & 0xf];
            }
            if (PyUnicodeWriter_WriteUTF8(writer, buf, len) < 0)
                goto bail;
        }
        run_start = i + 1;
    }
    if (i > run_start) {
        if (PyUnicodeWriter_WriteSubstring(writer, pystr, run_start, i) < 0)
            goto bail;
    }
    if (PyUnicodeWriter_WriteChar(writer, '"') < 0)
        goto bail;
    return PyUnicodeWriter_Finish(writer);
bail:
    PyUnicodeWriter_Discard(writer);
    return NULL;
}
#else /* PY_VERSION_HEX < 0x030E0000 */
static PyObject *
escape_unicode_noascii(PyObject *pystr)
{
    /* Two-pass: compute size, then fill. */
    Py_ssize_t i;
    Py_ssize_t input_chars = PyUnicode_GET_LENGTH(pystr);
    PY2_UNUSED int kind = PyUnicode_KIND(pystr);
    void *data = PyUnicode_DATA(pystr);
    Py_ssize_t output_size = 2; /* opening and closing quotes */
    PyObject *rval;

    for (i = 0; i < input_chars; i++) {
        JSON_UNICHR c = PyUnicode_READ(kind, data, i);
        Py_ssize_t charsize = escape_char_noascii_size(c);
        if (output_size > PY_SSIZE_T_MAX - charsize) {
            PyErr_SetString(PyExc_OverflowError,
                            "string is too long to escape");
            return NULL;
        }
        output_size += charsize;
    }

#if PY_MAJOR_VERSION >= 3
    {
        /* Escapes are all ASCII, so maxchar doesn't increase */
        Py_UCS4 maxchar = PyUnicode_MAX_CHAR_VALUE(pystr);
        if (maxchar < 127)
            maxchar = 127;
        rval = PyUnicode_New(output_size, maxchar);
        if (rval == NULL)
            return NULL;
    }
    {
        int out_kind = PyUnicode_KIND(rval);
        void *out_data = PyUnicode_DATA(rval);
        Py_ssize_t chars = 0;
        PyUnicode_WRITE(out_kind, out_data, chars++, '"');
        for (i = 0; i < input_chars; i++) {
            JSON_UNICHR c = PyUnicode_READ(kind, data, i);
            if (!NEEDS_ESCAPE(c)) {
                PyUnicode_WRITE(out_kind, out_data, chars++, c);
            }
            else {
                JSON_UNICHR buf[6];
                Py_ssize_t j, n = escape_char_noascii(c, buf, 0);
                for (j = 0; j < n; j++)
                    PyUnicode_WRITE(out_kind, out_data, chars++, buf[j]);
            }
        }
        PyUnicode_WRITE(out_kind, out_data, chars++, '"');
        assert(chars == output_size);
    }
#else
    /* Python 2: return a unicode object */
    rval = PyUnicode_FromUnicode(NULL, output_size);
    if (rval == NULL)
        return NULL;
    {
        Py_UNICODE *output = PyUnicode_AS_UNICODE(rval);
        Py_ssize_t chars = 0;
        output[chars++] = '"';
        for (i = 0; i < input_chars; i++) {
            chars = escape_char_noascii(
                PyUnicode_READ(kind, data, i), output, chars);
        }
        output[chars++] = '"';
        assert(chars == output_size);
    }
#endif
    return rval;
}
#endif /* PY_VERSION_HEX >= 0x030E0000 */

static PyObject *
py_encode_basestring(PyObject* self UNUSED, PyObject *pystr)
{
    /* Return a JSON representation of a Python string (ensure_ascii=False) */
    /* METH_O */
    if (PyBytes_Check(pystr)) {
        PyObject *uni;
        PyObject *rval;
        uni = PyUnicode_DecodeUTF8(
            PyBytes_AS_STRING(pystr), PyBytes_GET_SIZE(pystr), NULL);
        if (uni == NULL)
            return NULL;
        rval = escape_unicode_noascii(uni);
        Py_DECREF(uni);
        return rval;
    }
    else if (PyUnicode_Check(pystr)) {
        if (PyUnicode_READY(pystr))
            return NULL;
        return escape_unicode_noascii(pystr);
    }
    else {
        PyErr_Format(PyExc_TypeError,
                     "first argument must be a string, not %.80s",
                     Py_TYPE(pystr)->tp_name);
        return NULL;
    }
}

static void
scanner_dealloc(PyObject *self)
{
    /* bpo-31095: UnTrack is needed before calling any callbacks */
#if PY_VERSION_HEX >= 0x030D0000
    PyTypeObject *tp = Py_TYPE(self);
#endif
    PyObject_GC_UnTrack(self);
    scanner_clear(self);
    Py_TYPE(self)->tp_free(self);
#if PY_VERSION_HEX >= 0x030D0000
    Py_DECREF(tp);
#endif
}

static int
scanner_traverse(PyObject *self, visitproc visit, void *arg)
{
    PyScannerObject *s = (PyScannerObject *)self;
#if PY_VERSION_HEX >= 0x030D0000
    /* Heap types must visit their type for GC. */
    Py_VISIT(Py_TYPE(self));
#else
    assert(PyScanner_Check(self));
#endif
#define JSON_VISIT_FIELD(f) Py_VISIT(s->f);
    JSON_SCANNER_OBJECT_FIELDS(JSON_VISIT_FIELD)
#undef JSON_VISIT_FIELD
    return 0;
}

static int
scanner_clear(PyObject *self)
{
    PyScannerObject *s = (PyScannerObject *)self;
#if PY_VERSION_HEX < 0x030D0000
    assert(PyScanner_Check(self));
#endif
#define JSON_CLEAR_FIELD(f) Py_CLEAR(s->f);
    JSON_SCANNER_OBJECT_FIELDS(JSON_CLEAR_FIELD)
#undef JSON_CLEAR_FIELD
    return 0;
}

static PyObject *
_parse_constant(PyScannerObject *s, PyObject *pystr, PyObject *constant,
                Py_ssize_t idx, Py_ssize_t *next_idx_ptr)
{
    /* Read a JSON constant from pystr. `constant` is the Python string
       that was found ("NaN", "Infinity", "-Infinity"). Returns the
       result of s->parse_constant(constant). */
    PyObject *rval;
    if (s->parse_constant == Py_None) {
        raise_errmsg(get_speedups_state(s->module_ref),
                     ERR_EXPECTING_VALUE, pystr, idx);
        return NULL;
    }

    rval = PyObject_CallOneArg(s->parse_constant, constant);
    idx += PyString_GET_SIZE(constant);
    *next_idx_ptr = idx;
    return rval;
}

/* -- Helper functions for _match_number fast paths (used by the
   _speedups_scan.h template). Factored out so the template can stay
   agnostic about PyFloat / PyInt vs PyObject_CallOneArg details. */

static inline PyObject *
_match_number_float_fast_unicode(PyObject *numstr)
{
#if PY_MAJOR_VERSION >= 3
    return PyFloat_FromString(numstr);
#else
    return PyFloat_FromString(numstr, NULL);
#endif
}

static inline PyObject *
_match_number_int_fast_unicode(PyScannerObject *s, PyObject *numstr)
{
    /* No fast path for unicode -> int; always call parse_int. */
    return PyObject_CallOneArg(s->parse_int, numstr);
}

#if PY_MAJOR_VERSION < 3
static inline PyObject *
_match_number_float_fast_str(PyObject *numstr)
{
    double d = PyOS_string_to_double(PyString_AS_STRING(numstr), NULL, NULL);
    if (d == -1.0 && PyErr_Occurred())
        return NULL;
    return PyFloat_FromDouble(d);
}

static inline PyObject *
_match_number_int_fast_str(PyScannerObject *s, PyObject *numstr)
{
    if (s->parse_int != (PyObject *)&PyInt_Type) {
        return PyObject_CallOneArg(s->parse_int, numstr);
    }
    return PyInt_FromString(PyString_AS_STRING(numstr), NULL, 10);
}
#endif

/* -- Generate scan_once_unicode, _parse_object_unicode, _parse_array_unicode,
   _match_number_unicode from the shared template. -- */
#define JSON_SCAN_SUFFIX _unicode
#define JSON_SCAN_DATA_INIT(p) \
    PY2_UNUSED int kind = PyUnicode_KIND(p); \
    void *str = PyUnicode_DATA(p); \
    Py_ssize_t end_idx = PyUnicode_GET_LENGTH(p) - 1
#define JSON_SCAN_READ(i) PyUnicode_READ(kind, str, (i))
#define JSON_SCAN_SCANSTRING_CALL(pos, nextp) \
    scanstring_unicode(state, pystr, (pos), s->strict, (nextp))
#if PY_MAJOR_VERSION >= 3
#define JSON_SCAN_NUMSTR_CREATE(sidx, eidx) \
    PyUnicode_Substring(pystr, (sidx), (eidx))
#else
#define JSON_SCAN_NUMSTR_CREATE(sidx, eidx) \
    PyUnicode_FromUnicode(&((Py_UNICODE *)str)[(sidx)], (eidx) - (sidx))
#endif
#define JSON_SCAN_PARSE_FLOAT_FAST(ns) _match_number_float_fast_unicode(ns)
#define JSON_SCAN_PARSE_INT_FAST(ns)   _match_number_int_fast_unicode(s, ns)
#define JSON_SPEEDUPS_SCAN_INCLUDING 1
#include "_speedups_scan.h"
#undef JSON_SPEEDUPS_SCAN_INCLUDING

/* -- Generate the corresponding _str variants on Python 2. -- */
#if PY_MAJOR_VERSION < 3
#define JSON_SCAN_SUFFIX _str
#define JSON_SCAN_DATA_INIT(p) \
    char *str = PyString_AS_STRING(p); \
    Py_ssize_t end_idx = PyString_GET_SIZE(p) - 1
#define JSON_SCAN_READ(i) ((unsigned char)str[(i)])
#define JSON_SCAN_SCANSTRING_CALL(pos, nextp) \
    scanstring_str(state, pystr, (pos), \
                   PyString_AS_STRING(s->encoding), s->strict, (nextp))
#define JSON_SCAN_NUMSTR_CREATE(sidx, eidx) \
    PyString_FromStringAndSize(&str[(sidx)], (eidx) - (sidx))
#define JSON_SCAN_PARSE_FLOAT_FAST(ns) _match_number_float_fast_str(ns)
#define JSON_SCAN_PARSE_INT_FAST(ns)   _match_number_int_fast_str(s, ns)
#define JSON_SPEEDUPS_SCAN_INCLUDING 1
#include "_speedups_scan.h"
#undef JSON_SPEEDUPS_SCAN_INCLUDING
#endif /* PY_MAJOR_VERSION < 3 */


static PyObject *
scanner_call(PyObject *self, PyObject *args, PyObject *kwds)
{
    /* Python callable interface to scan_once_{str,unicode} */
    PyObject *pystr;
    PyObject *rval = NULL;
    Py_ssize_t idx;
    Py_ssize_t next_idx = -1;
    static char *kwlist[] = {"string", "idx", NULL};
    PyScannerObject *s;
#if PY_VERSION_HEX < 0x030D0000
    assert(PyScanner_Check(self));
#endif
    s = (PyScannerObject *)self;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "On:scan_once", kwlist, &pystr, &idx))
        return NULL;

    if (PyUnicode_Check(pystr)) {
        if (PyUnicode_READY(pystr))
            return NULL;
    }
#if PY_MAJOR_VERSION < 3
    else if (!PyString_Check(pystr)) {
#else
    else {
#endif
        PyErr_Format(PyExc_TypeError,
                 "first argument must be a string, not %.80s",
                 Py_TYPE(pystr)->tp_name);
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);

    if (PyUnicode_Check(pystr)) {
        rval = scan_once_unicode(s, pystr, idx, &next_idx);
    }
#if PY_MAJOR_VERSION < 3
    else {
        rval = scan_once_str(s, pystr, idx, &next_idx);
    }
#endif /* PY_MAJOR_VERSION < 3 */
    PyDict_Clear(s->memo);

    Py_END_CRITICAL_SECTION();
    return _build_rval_index_tuple(rval, next_idx);
}

static PyObject *
JSON_ParseEncoding(PyObject *encoding)
{
    if (encoding == Py_None)
        return JSON_InternFromString(DEFAULT_ENCODING);
#if PY_MAJOR_VERSION >= 3
    if (PyUnicode_Check(encoding)) {
        if (PyUnicode_AsUTF8(encoding) == NULL) {
            return NULL;
        }
        Py_INCREF(encoding);
        return encoding;
    }
#else /* PY_MAJOR_VERSION >= 3 */
    if (PyString_Check(encoding)) {
        Py_INCREF(encoding);
        return encoding;
    }
    if (PyUnicode_Check(encoding))
        return PyUnicode_AsEncodedString(encoding, NULL, NULL);
#endif /* PY_MAJOR_VERSION >= 3 */
    PyErr_SetString(PyExc_TypeError, "encoding must be a string");
    return NULL;
}

static PyObject *
scanner_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    /* Initialize Scanner object */
    PyObject *ctx;
    static char *kwlist[] = {"context", NULL};
    PyScannerObject *s;
    PyObject *encoding;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O:make_scanner", kwlist, &ctx))
        return NULL;

    s = (PyScannerObject *)type->tp_alloc(type, 0);
    if (s == NULL)
        return NULL;

#if PY_VERSION_HEX >= 0x030D0000
    s->module_ref = PyType_GetModuleByDef(type, &moduledef);
    if (s->module_ref == NULL)
        goto bail;
#else
    s->module_ref = _speedups_module;  /* borrowed */
#endif
    Py_INCREF(s->module_ref);

    if (s->memo == NULL) {
        s->memo = PyDict_New();
        if (s->memo == NULL)
            goto bail;
    }

    /* Load required attributes from the Python-side JSONDecoder context.
     * Each getattr failure is a hard error; goto bail lets scanner_dealloc
     * release whatever we managed to set on s. */
#define LOAD_ATTR(field, name)                              \
    do {                                                    \
        s->field = PyObject_GetAttrString(ctx, name);       \
        if (s->field == NULL)                               \
            goto bail;                                      \
    } while (0)

    encoding = PyObject_GetAttrString(ctx, "encoding");
    if (encoding == NULL)
        goto bail;
    s->encoding = JSON_ParseEncoding(encoding);
    Py_XDECREF(encoding);
    if (s->encoding == NULL)
        goto bail;

    LOAD_ATTR(strict_bool, "strict");
    s->strict = PyObject_IsTrue(s->strict_bool);
    if (s->strict < 0)
        goto bail;
    LOAD_ATTR(object_hook, "object_hook");
    LOAD_ATTR(pairs_hook, "object_pairs_hook");
    LOAD_ATTR(array_hook, "array_hook");
    LOAD_ATTR(parse_float, "parse_float");
    LOAD_ATTR(parse_int, "parse_int");
    LOAD_ATTR(parse_constant, "parse_constant");

#undef LOAD_ATTR

    return (PyObject *)s;

bail:
    Py_DECREF(s);
    return NULL;
}

PyDoc_STRVAR(scanner_doc, "JSON scanner object");

#if PY_VERSION_HEX >= 0x030D0000
/* Heap type slots and spec for Python 3.13+ */
static PyType_Slot PyScannerType_slots[] = {
    {Py_tp_doc, (void *)scanner_doc},
    {Py_tp_dealloc, scanner_dealloc},
    {Py_tp_call, scanner_call},
    {Py_tp_traverse, scanner_traverse},
    {Py_tp_clear, scanner_clear},
    {Py_tp_members, scanner_members},
    {Py_tp_new, scanner_new},
    {0, NULL}
};

static PyType_Spec PyScannerType_spec = {
    .name = "simplejson._speedups.Scanner",
    .basicsize = sizeof(PyScannerObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = PyScannerType_slots,
};
#else
static PyTypeObject PyScannerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "simplejson._speedups.Scanner",       /* tp_name */
    sizeof(PyScannerObject), /* tp_basicsize */
    0,                    /* tp_itemsize */
    scanner_dealloc, /* tp_dealloc */
    0,                    /* tp_print */
    0,                    /* tp_getattr */
    0,                    /* tp_setattr */
    0,                    /* tp_compare */
    0,                    /* tp_repr */
    0,                    /* tp_as_number */
    0,                    /* tp_as_sequence */
    0,                    /* tp_as_mapping */
    0,                    /* tp_hash */
    scanner_call,         /* tp_call */
    0,                    /* tp_str */
    0,/* PyObject_GenericGetAttr, */                    /* tp_getattro */
    0,/* PyObject_GenericSetAttr, */                    /* tp_setattro */
    0,                    /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,   /* tp_flags */
    scanner_doc,          /* tp_doc */
    scanner_traverse,                    /* tp_traverse */
    scanner_clear,                    /* tp_clear */
    0,                    /* tp_richcompare */
    0,                    /* tp_weaklistoffset */
    0,                    /* tp_iter */
    0,                    /* tp_iternext */
    0,                    /* tp_methods */
    scanner_members,                    /* tp_members */
    0,                    /* tp_getset */
    0,                    /* tp_base */
    0,                    /* tp_dict */
    0,                    /* tp_descr_get */
    0,                    /* tp_descr_set */
    0,                    /* tp_dictoffset */
    0,                    /* tp_init */
    0,/* PyType_GenericAlloc, */        /* tp_alloc */
    scanner_new,          /* tp_new */
    0,/* PyObject_GC_Del, */              /* tp_free */
};
#endif

static PyObject *
encoder_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {
        "markers",
        "default",
        "encoder",
        "indent",
        "key_separator",
        "item_separator",
        "sort_keys",
        "skipkeys",
        "allow_nan",
        "key_memo",
        "use_decimal",
        "namedtuple_as_object",
        "tuple_as_array",
        "int_as_string_bitcount",
        "item_sort_key",
        "encoding",
        "for_json",
        "ignore_nan",
        "Decimal",
        "iterable_as_array",
        NULL};

    PyEncoderObject *s;
    PyObject *markers, *defaultfn, *encoder, *indent, *key_separator;
    PyObject *item_separator, *sort_keys, *skipkeys, *allow_nan, *key_memo;
    PyObject *use_decimal, *namedtuple_as_object, *tuple_as_array, *iterable_as_array;
    PyObject *int_as_string_bitcount, *item_sort_key, *encoding, *for_json;
    PyObject *ignore_nan, *Decimal;
    int is_true;

    /* Build the format string from per-argument pieces so that each "O"
     * has a comment and adding/removing an argument only touches one
     * line instead of counting letters in a 20-char string literal. The
     * order here must match kwlist[] above and the &var argument list
     * to PyArg_ParseTupleAndKeywords below. */
    static const char *const fmt =
        "O"  /* markers */
        "O"  /* default */
        "O"  /* encoder */
        "O"  /* indent */
        "O"  /* key_separator */
        "O"  /* item_separator */
        "O"  /* sort_keys */
        "O"  /* skipkeys */
        "O"  /* allow_nan */
        "O"  /* key_memo */
        "O"  /* use_decimal */
        "O"  /* namedtuple_as_object */
        "O"  /* tuple_as_array */
        "O"  /* int_as_string_bitcount */
        "O"  /* item_sort_key */
        "O"  /* encoding */
        "O"  /* for_json */
        "O"  /* ignore_nan */
        "O"  /* Decimal */
        "O"  /* iterable_as_array */
        ":make_encoder";

    if (!PyArg_ParseTupleAndKeywords(args, kwds, fmt, kwlist,
        &markers, &defaultfn, &encoder, &indent, &key_separator, &item_separator,
        &sort_keys, &skipkeys, &allow_nan, &key_memo, &use_decimal,
        &namedtuple_as_object, &tuple_as_array,
        &int_as_string_bitcount, &item_sort_key, &encoding, &for_json,
        &ignore_nan, &Decimal, &iterable_as_array))
        return NULL;

    s = (PyEncoderObject *)type->tp_alloc(type, 0);
    if (s == NULL)
        return NULL;

#if PY_VERSION_HEX >= 0x030D0000
    s->module_ref = PyType_GetModuleByDef(type, &moduledef);
    if (s->module_ref == NULL)
        goto bail;
#else
    s->module_ref = _speedups_module;  /* borrowed */
#endif
    Py_INCREF(s->module_ref);

    Py_INCREF(markers);
    s->markers = markers;
    Py_INCREF(defaultfn);
    s->defaultfn = defaultfn;
    Py_INCREF(encoder);
    s->encoder = encoder;
#if PY_MAJOR_VERSION >= 3
    if (encoding == Py_None) {
        /* Py3: encoding=None means "don't decode bytes keys/values".
         * Store Py_None rather than NULL so Py_T_OBJECT_EX exposes the
         * attribute as None (not AttributeError) and so tp_traverse /
         * tp_clear handle the slot uniformly. The bytes-path sentinel
         * checks in encoder_stringify_key and encoder_listencode_obj
         * compare against Py_None. */
        Py_INCREF(Py_None);
        s->encoding = Py_None;
    }
    else
#endif /* PY_MAJOR_VERSION >= 3 */
    {
        s->encoding = JSON_ParseEncoding(encoding);
        if (s->encoding == NULL)
            goto bail;
    }
    Py_INCREF(indent);
    s->indent = indent;
    Py_INCREF(key_separator);
    s->key_separator = key_separator;
    Py_INCREF(item_separator);
    s->item_separator = item_separator;
    Py_INCREF(skipkeys);
    s->skipkeys_bool = skipkeys;
    s->skipkeys = PyObject_IsTrue(skipkeys);
    if (s->skipkeys < 0)
        goto bail;
    Py_INCREF(key_memo);
    s->key_memo = key_memo;
    s->fast_encode = (PyCFunction_Check(s->encoder) && PyCFunction_GetFunction(s->encoder) == (PyCFunction)py_encode_basestring_ascii);
    is_true = PyObject_IsTrue(ignore_nan);
    if (is_true < 0)
        goto bail;
    s->allow_or_ignore_nan = is_true ? JSON_IGNORE_NAN : 0;
    is_true = PyObject_IsTrue(allow_nan);
    if (is_true < 0)
        goto bail;
    s->allow_or_ignore_nan |= is_true ? JSON_ALLOW_NAN : 0;
    s->use_decimal = PyObject_IsTrue(use_decimal);
    if (s->use_decimal < 0)
        goto bail;
    s->namedtuple_as_object = PyObject_IsTrue(namedtuple_as_object);
    if (s->namedtuple_as_object < 0)
        goto bail;
    s->tuple_as_array = PyObject_IsTrue(tuple_as_array);
    if (s->tuple_as_array < 0)
        goto bail;
    s->iterable_as_array = PyObject_IsTrue(iterable_as_array);
    if (s->iterable_as_array < 0)
        goto bail;
    if (PyInt_Check(int_as_string_bitcount) || PyLong_Check(int_as_string_bitcount)) {
        static const unsigned long long_long_bitsize = sizeof(long long) * CHAR_BIT;
        long int_as_string_bitcount_val = PyLong_AsLong(int_as_string_bitcount);
        if (int_as_string_bitcount_val == -1 && PyErr_Occurred())
            goto bail;
        if (int_as_string_bitcount_val > 0 && int_as_string_bitcount_val < (long)long_long_bitsize) {
            int n = (int)int_as_string_bitcount_val;
            /* Compute 2^n as unsigned (well-defined for n < 64) and
             * -(2^n) as signed without UB. Naive "-1LL << n" is a
             * shift of a negative value, which is undefined, and
             * "-(1LL << n)" overflows when n == 63. The expression
             * below avoids both: (1ULL << n) - 1 is always >= 0, so
             * negating it and subtracting 1 stays in range and
             * produces LLONG_MIN at n == 63. */
            s->max_long_size = PyLong_FromUnsignedLongLong(1ULL << n);
            s->min_long_size = PyLong_FromLongLong(
                -(long long)((1ULL << n) - 1ULL) - 1LL);
            if (s->min_long_size == NULL || s->max_long_size == NULL) {
                goto bail;
            }
        }
        else {
            PyErr_Format(PyExc_TypeError,
                         "int_as_string_bitcount (%ld) must be greater than 0 and less than the number of bits of a `long long` type (%lu bits)",
                         int_as_string_bitcount_val, long_long_bitsize);
            goto bail;
        }
    }
    else if (int_as_string_bitcount == Py_None) {
        Py_INCREF(Py_None);
        s->max_long_size = Py_None;
        Py_INCREF(Py_None);
        s->min_long_size = Py_None;
    }
    else {
        PyErr_SetString(PyExc_TypeError, "int_as_string_bitcount must be None or an integer");
        goto bail;
    }
    if (item_sort_key != Py_None) {
        if (!PyCallable_Check(item_sort_key)) {
            PyErr_SetString(PyExc_TypeError, "item_sort_key must be None or callable");
            goto bail;
        }
    }
    else {
        is_true = PyObject_IsTrue(sort_keys);
        if (is_true < 0)
            goto bail;
        if (is_true) {
            _speedups_state *state = get_speedups_state(s->module_ref);
            item_sort_key = state->JSON_itemgetter0;
            if (!item_sort_key)
                goto bail;
        }
    }
    if (item_sort_key == Py_None) {
        Py_INCREF(Py_None);
        s->item_sort_kw = Py_None;
    }
    else {
        s->item_sort_kw = PyDict_New();
        if (s->item_sort_kw == NULL)
            goto bail;
        if (PyDict_SetItemString(s->item_sort_kw, "key", item_sort_key))
            goto bail;
    }
    Py_INCREF(sort_keys);
    s->sort_keys = sort_keys;
    Py_INCREF(item_sort_key);
    s->item_sort_key = item_sort_key;
    Py_INCREF(Decimal);
    s->Decimal = Decimal;
    s->for_json = PyObject_IsTrue(for_json);
    if (s->for_json < 0)
        goto bail;

    return (PyObject *)s;

bail:
    Py_DECREF(s);
    return NULL;
}

static PyObject *
encoder_call(PyObject *self, PyObject *args, PyObject *kwds)
{
    /* Python callable interface to encode_listencode_obj */
    static char *kwlist[] = {"obj", "_current_indent_level", NULL};
    PyObject *obj;
    Py_ssize_t indent_level;
    PyEncoderObject *s;
    JSON_Accu rval;
    _speedups_state *state;
    int encode_rv;
#if PY_VERSION_HEX < 0x030D0000
    assert(PyEncoder_Check(self));
#endif
    s = (PyEncoderObject *)self;
    state = get_speedups_state(s->module_ref);
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "On:_iterencode", kwlist,
        &obj, &indent_level))
        return NULL;
    if (JSON_Accu_Init(&rval))
        return NULL;
    Py_BEGIN_CRITICAL_SECTION(self);
    encode_rv = encoder_listencode_obj(s, &rval, obj, indent_level);
    Py_END_CRITICAL_SECTION();
    if (encode_rv) {
        JSON_Accu_Destroy(&rval);
        return NULL;
    }
    return JSON_Accu_FinishAsList(state, &rval);
}

static PyObject *
_encoded_const(_speedups_state *state, PyObject *obj)
{
    /* Return the JSON string representation of None, True, False */
    if (obj == Py_None) {
        Py_INCREF(state->JSON_s_null);
        return state->JSON_s_null;
    }
    else if (obj == Py_True) {
        Py_INCREF(state->JSON_s_true);
        return state->JSON_s_true;
    }
    else if (obj == Py_False) {
        Py_INCREF(state->JSON_s_false);
        return state->JSON_s_false;
    }
    else {
        PyErr_SetString(PyExc_ValueError, "not a const");
        return NULL;
    }
}

static PyObject *
encoder_encode_float(PyEncoderObject *s, PyObject *obj)
{
    /* Return the JSON representation of a PyFloat */
    _speedups_state *state = get_speedups_state(s->module_ref);
    double i = PyFloat_AS_DOUBLE(obj);
    if (!Py_IS_FINITE(i)) {
        if (!s->allow_or_ignore_nan) {
            PyErr_SetString(PyExc_ValueError, "Out of range float values are not JSON compliant");
            return NULL;
        }
        if (s->allow_or_ignore_nan & JSON_IGNORE_NAN) {
            return _encoded_const(state, Py_None);
        }
        /* JSON_ALLOW_NAN is set */
        else if (i > 0) {
            Py_INCREF(state->JSON_Infinity);
            return state->JSON_Infinity;
        }
        else if (i < 0) {
            Py_INCREF(state->JSON_NegInfinity);
            return state->JSON_NegInfinity;
        }
        else {
            Py_INCREF(state->JSON_NaN);
            return state->JSON_NaN;
        }
    }
    /* Use a better float format here? */
    if (PyFloat_CheckExact(obj)) {
        return PyObject_Repr(obj);
    }
    else {
        /* See #118, do not trust custom str/repr */
        PyObject *res;
        PyObject *tmp = PyObject_CallOneArg((PyObject *)&PyFloat_Type, obj);
        if (tmp == NULL) {
            return NULL;
        }
        res = PyObject_Repr(tmp);
        Py_DECREF(tmp);
        return res;
    }
}

static PyObject *
encoder_encode_string(PyEncoderObject *s, PyObject *obj)
{
    /* Return the JSON representation of a string */
    PyObject *encoded;

    if (s->fast_encode) {
        return py_encode_basestring_ascii(NULL, obj);
    }
    encoded = PyObject_CallOneArg(s->encoder, obj);
    if (encoded != NULL &&
#if PY_MAJOR_VERSION < 3
        !PyString_Check(encoded) &&
#endif /* PY_MAJOR_VERSION < 3 */
        !PyUnicode_Check(encoded))
    {
        PyErr_Format(PyExc_TypeError,
                     "encoder() must return a string, not %.80s",
                     Py_TYPE(encoded)->tp_name);
        Py_DECREF(encoded);
        return NULL;
    }
    return encoded;
}

static int
_steal_accumulate(_speedups_state *state, JSON_Accu *accu, PyObject *stolen)
{
    /* Append stolen and then decrement its reference count */
    int rval = JSON_Accu_Accumulate(state, accu, stolen);
    Py_DECREF(stolen);
    return rval;
}

/* Push a reference to `obj` into the encoder's circular-reference marker
 * dict, keyed by the object's address. Allocates a fresh PyLong ident and
 * stores it in *ident_ptr (caller owns the reference, and must pass it to
 * encoder_markers_pop to remove the entry). If markers tracking is off
 * (s->markers == Py_None), stores NULL in *ident_ptr and returns 0 with
 * no side effects — that NULL is the sentinel markers_pop understands.
 * On circular reference sets the ValueError; on any other failure sets
 * the corresponding exception. Returns 0 on success, -1 on error. */
static int
encoder_markers_push(PyEncoderObject *s, PyObject *obj, PyObject **ident_ptr)
{
    PyObject *ident;
    int has_key;
    *ident_ptr = NULL;
    if (s->markers == Py_None)
        return 0;
    ident = PyLong_FromVoidPtr(obj);
    if (ident == NULL)
        return -1;
    has_key = PyDict_Contains(s->markers, ident);
    if (has_key) {
        if (has_key != -1)
            PyErr_SetString(PyExc_ValueError, "Circular reference detected");
        Py_DECREF(ident);
        return -1;
    }
    if (PyDict_SetItem(s->markers, ident, obj) < 0) {
        Py_DECREF(ident);
        return -1;
    }
    *ident_ptr = ident;
    return 0;
}

/* Counterpart to encoder_markers_push. Removes the ident entry from
 * s->markers and drops the caller's reference. Passing NULL is a no-op
 * so callers can invoke it unconditionally on the happy path (when
 * markers tracking was off, push wrote NULL into their local). Returns
 * 0 on success, -1 if PyDict_DelItem failed (the reference is still
 * released in that case). */
static int
encoder_markers_pop(PyEncoderObject *s, PyObject *ident)
{
    int rv;
    if (ident == NULL)
        return 0;
    rv = PyDict_DelItem(s->markers, ident);
    Py_DECREF(ident);
    return rv;
}

/* Helper for the for_json / _asdict paths in encoder_listencode_obj.
 * Steals the reference to `newobj` (returned by _call_json_method),
 * handles recursion-depth tracking, and dispatches to the right
 * sub-encoder:
 *   - if as_dict is 0, encodes newobj as a generic JSON value via
 *     encoder_listencode_obj (for_json contract: return any JSON-
 *     compatible value);
 *   - if as_dict is 1, encodes newobj as a dict via
 *     encoder_listencode_dict after a TypeError-on-mismatch check
 *     (_asdict contract: must return a dict).
 * Cleans up on every exit path. */
static int
encoder_steal_encode(PyEncoderObject *s, JSON_Accu *rval,
                     PyObject *newobj, Py_ssize_t indent_level,
                     int as_dict)
{
    int rv;
    if (newobj == NULL)
        return -1;
    if (Py_EnterRecursiveCall(" while encoding a JSON object")) {
        Py_DECREF(newobj);
        return -1;
    }
    if (as_dict) {
        if (JSON_AnyDict_Check(newobj)) {
            rv = encoder_listencode_dict(s, rval, newobj, indent_level);
        } else {
            PyErr_Format(PyExc_TypeError,
                         "_asdict() must return a dict, not %.80s",
                         Py_TYPE(newobj)->tp_name);
            rv = -1;
        }
    } else {
        rv = encoder_listencode_obj(s, rval, newobj, indent_level);
    }
    Py_DECREF(newobj);
    Py_LeaveRecursiveCall();
    return rv;
}

/* Fallback encoder path used when obj is not one of the directly-
 * supported JSON types (const, string, int, float, list, dict,
 * Decimal, etc.) and is not a _asdict / for_json candidate. Handles
 * three sub-cases in order:
 *   1. RawJSON — emit the already-encoded string verbatim.
 *   2. iterable_as_array — treat any iterable object as a JSON array.
 *   3. default(obj) — call the user-supplied default hook and recurse
 *      on its result, with circular-reference tracking via markers.
 * Returns 0 on success, -1 on error. */
static int
encoder_listencode_default(PyEncoderObject *s, JSON_Accu *rval,
                           PyObject *obj, Py_ssize_t indent_level)
{
    _speedups_state *state = get_speedups_state(s->module_ref);
    PyObject *ident = NULL;
    PyObject *newobj;
    int raw;
    int rv;

    raw = is_raw_json(state, obj);
    if (raw < 0)
        return -1;
    if (raw) {
        PyObject *encoded = PyObject_GetAttr(obj, state->JSON_attr_encoded_json);
        if (encoded == NULL)
            return -1;
        return _steal_accumulate(state, rval, encoded);
    }

    if (s->iterable_as_array) {
        newobj = PyObject_GetIter(obj);
        if (newobj == NULL) {
            if (!PyErr_ExceptionMatches(PyExc_TypeError))
                return -1;
            PyErr_Clear();
        } else {
            rv = encoder_listencode_list(s, rval, newobj, indent_level);
            Py_DECREF(newobj);
            return rv;
        }
    }

    if (encoder_markers_push(s, obj, &ident))
        return -1;
    if (Py_EnterRecursiveCall(" while encoding a JSON object")) {
        Py_XDECREF(ident);
        return -1;
    }
    newobj = PyObject_CallOneArg(s->defaultfn, obj);
    if (newobj == NULL) {
#if PY_VERSION_HEX >= 0x030B0000
        /* Annotate before unwinding; the "when serializing X object"
         * note uses the original obj's type name, matching the Python
         * encoder which binds type(o).__name__ before `o = default(o)`
         * runs. */
        encoder_annotate_exception(state,
            "when serializing %s object", Py_TYPE(obj)->tp_name);
#endif
        Py_LeaveRecursiveCall();
        Py_XDECREF(ident);
        return -1;
    }
    rv = encoder_listencode_obj(s, rval, newobj, indent_level);
    Py_LeaveRecursiveCall();
    if (rv) {
#if PY_VERSION_HEX >= 0x030B0000
        /* default() succeeded but encoding its return value failed; in
         * the Python encoder `o` has been rebound to newobj at this
         * point, so the note reflects that type. */
        encoder_annotate_exception(state,
            "when serializing %s object", Py_TYPE(newobj)->tp_name);
#endif
    }
    Py_DECREF(newobj);
    if (rv == 0) {
        if (encoder_markers_pop(s, ident) < 0)
            rv = -1;
    } else {
        Py_XDECREF(ident);
    }
    return rv;
}

static int
encoder_listencode_obj(PyEncoderObject *s, JSON_Accu *rval, PyObject *obj, Py_ssize_t indent_level)
{
    /* Encode Python object obj to a JSON term, rval is a PyList */
    _speedups_state *state = get_speedups_state(s->module_ref);
    PyObject *newobj;
    int rv = -1;
    /* Check strings first — they are the most common JSON value type. */
    if ((PyBytes_Check(obj) && s->encoding != Py_None) ||
        PyUnicode_Check(obj))
    {
        PyObject *encoded = encoder_encode_string(s, obj);
        if (encoded != NULL)
            rv = _steal_accumulate(state, rval, encoded);
    }
    else if (obj == Py_None || obj == Py_True || obj == Py_False) {
        PyObject *cstr = _encoded_const(state, obj);
        if (cstr != NULL)
            rv = _steal_accumulate(state, rval, cstr);
    }
    else if (PyInt_Check(obj) || PyLong_Check(obj)) {
        PyObject *encoded = encoder_long_to_str(obj);
        if (encoded != NULL) {
            encoded = maybe_quote_bigint(s, encoded, obj);
            if (encoded != NULL)
                rv = _steal_accumulate(state, rval, encoded);
        }
    }
    else if (PyFloat_Check(obj)) {
        PyObject *encoded = encoder_encode_float(s, obj);
        if (encoded != NULL)
            rv = _steal_accumulate(state, rval, encoded);
    }
    else if (s->for_json && _call_json_method(obj, state->JSON_attr_for_json, &newobj)) {
        rv = encoder_steal_encode(s, rval, newobj, indent_level, /*as_dict=*/0);
    }
    else if (s->namedtuple_as_object && _call_json_method(obj, state->JSON_attr_asdict, &newobj)) {
        rv = encoder_steal_encode(s, rval, newobj, indent_level, /*as_dict=*/1);
    }
    else if (PyList_Check(obj) || (s->tuple_as_array && PyTuple_Check(obj))) {
        if (Py_EnterRecursiveCall(" while encoding a JSON object"))
            return rv;
        rv = encoder_listencode_list(s, rval, obj, indent_level);
        Py_LeaveRecursiveCall();
    }
    else if (JSON_AnyDict_Check(obj)) {
        if (Py_EnterRecursiveCall(" while encoding a JSON object"))
            return rv;
        rv = encoder_listencode_dict(s, rval, obj, indent_level);
        Py_LeaveRecursiveCall();
    }
    else if (s->use_decimal && PyObject_TypeCheck(obj, (PyTypeObject *)s->Decimal)) {
        PyObject *encoded = PyObject_Str(obj);
        if (encoded != NULL)
            rv = _steal_accumulate(state, rval, encoded);
    }
    else {
        rv = encoder_listencode_default(s, rval, obj, indent_level);
    }
    return rv;
}

/* Stringify and encode a dict key to its JSON representation, using the
 * key_memo cache for string keys.  Returns a new reference to the encoded
 * key string on success, Py_None (borrowed, no new reference) for
 * skipkeys, or NULL on error. */
static PyObject *
encoder_encode_dict_key(PyEncoderObject *s, PyObject *key)
{
    PyObject *kstr;
    PyObject *encoded;

    kstr = encoder_stringify_key(s, key);
    if (kstr == NULL)
        return NULL;
    if (kstr == Py_None) {
        Py_DECREF(kstr);
        return Py_None;  /* skipkeys */
    }

    /* For string keys (PyUnicode on Py3, PyString on Py2),
     * encoder_stringify_key returns Py_INCREF(key) — i.e. kstr IS key.
     * For non-string keys it returns a freshly created string, so
     * kstr != key.  Use this identity test to decide whether the
     * key_memo cache applies: caching under a non-string original key
     * would be write-only (the lookup uses kstr, not key). */
    if (kstr == key) {
        int cached = json_PyDict_GetItemRef(s->key_memo, kstr, &encoded);
        if (cached < 0) {
            Py_DECREF(kstr);
            return NULL;
        }
        if (cached == 0) {
            encoded = encoder_encode_string(s, kstr);
            if (encoded == NULL) {
                Py_DECREF(kstr);
                return NULL;
            }
            if (PyDict_SetItem(s->key_memo, key, encoded)) {
                Py_DECREF(kstr);
                Py_DECREF(encoded);
                return NULL;
            }
        }
        Py_DECREF(kstr);
    } else {
        encoded = encoder_encode_string(s, kstr);
        Py_DECREF(kstr);
        if (encoded == NULL)
            return NULL;
    }
    return encoded;  /* new reference */
}

/* Write '\n' followed by indent_level copies of s->indent directly to
 * the accumulator, without materializing the combined string as an
 * intermediate PyObject. This is the newline-plus-indent prefix
 * emitted before each item of an indented array or object and before
 * the matching closing bracket.
 *
 * Writing pieces rather than building a concatenated string saves a
 * PyUnicode_FromStringAndSize + PySequence_Repeat + PyNumber_Add per
 * container (versus the previous encoder_build_indent_string helper),
 * which matters most for deeply nested structures and on 3.14+ where
 * JSON_Accu is a PyUnicodeWriter that appends in-place at near-zero
 * per-write cost. On older Python the cost is one PyList_Append per
 * piece; the extra list entries are cheap compared to the allocation
 * traffic and the PySequence_Repeat's proportional memcpy work.
 *
 * Returns 0 on success, -1 on allocation failure. Only called when
 * s->indent is not Py_None. */
static int
encoder_accumulate_newline_indent(PyEncoderObject *s, _speedups_state *state,
                                  JSON_Accu *rval, Py_ssize_t indent_level)
{
    Py_ssize_t i;
    if (JSON_Accu_Accumulate(state, rval, state->JSON_newline))
        return -1;
    for (i = 0; i < indent_level; i++) {
        if (JSON_Accu_Accumulate(state, rval, s->indent))
            return -1;
    }
    return 0;
}

#if PY_VERSION_HEX >= 0x030B0000
/* Attach a PEP 678 note formatted via PyUnicode_FromFormatV to the in-
 * flight exception. Mirrors the pure-Python encoder's
 * `except BaseException as exc: exc.add_note(...); raise` wrappers
 * around each recursive encode step.
 *
 * Crucially, PyErr_Fetch runs BEFORE the note is built: the `%R`
 * formatter walks into PyObject_Repr, which on debug builds
 * (cpython-3.14-debug and cpython-3.14t+debug) asserts that no
 * exception is currently set. Building the note while our caller's
 * exception is still live would core-dump those jobs, as the CI
 * failure on job 72447820967 / 72447820982 demonstrated. Fetch first,
 * format second, Restore last.
 *
 * Failures on the note path — a repr() that raises, PyUnicode_FromFormatV
 * returning NULL, a BaseException subclass whose add_note override
 * raises — are swallowed so the in-flight exception survives intact.
 *
 * Only compiled on Python 3.11+, where PEP 678 exists. */
static void
encoder_annotate_exception(_speedups_state *state, const char *format, ...)
{
    PyObject *exc_type;
    PyObject *exc_value;
    PyObject *exc_tb;
    PyObject *note;
    PyObject *result;
    va_list args;

    PyErr_Fetch(&exc_type, &exc_value, &exc_tb);
    if (exc_value == NULL) {
        /* No live exception to annotate — callsites always have one,
         * so this is a defensive no-op. */
        PyErr_Restore(exc_type, exc_value, exc_tb);
        return;
    }
    PyErr_NormalizeException(&exc_type, &exc_value, &exc_tb);

    va_start(args, format);
    note = PyUnicode_FromFormatV(format, args);
    va_end(args);

    if (note != NULL) {
        result = PyObject_CallMethodObjArgs(exc_value, state->JSON_attr_add_note,
                                            note, NULL);
        Py_DECREF(note);
        if (result == NULL)
            PyErr_Clear();
        else
            Py_DECREF(result);
    }
    else {
        PyErr_Clear();
    }

    PyErr_Restore(exc_type, exc_value, exc_tb);
}
#endif

static int
encoder_listencode_dict(PyEncoderObject *s, JSON_Accu *rval, PyObject *dct, Py_ssize_t indent_level)
{
    /* Encode Python dict dct a JSON term */
    _speedups_state *state = get_speedups_state(s->module_ref);
    PyObject *ident = NULL;
    PyObject *encoded = NULL;
    PyObject *iter = NULL;
    PyObject *item = NULL;
    /* When s->indent is not Py_None, each inter-item boundary is emitted
     * as s->item_separator + '\n' + (s->indent * inner_indent_level) via
     * multiple JSON_Accu_Accumulate calls; no combined PyObject is
     * materialized. */
    int indented = (s->indent != Py_None);
    Py_ssize_t inner_indent_level = indented ? indent_level + 1 : indent_level;
    Py_ssize_t idx;

    {
        Py_ssize_t dct_size = PyDict_Check(dct) ? PyDict_Size(dct)
                                                : PyObject_Length(dct);
        if (dct_size == 0)
            return JSON_Accu_Accumulate(state, rval, state->JSON_empty_dict);
        if (dct_size < 0)
            return -1;
    }

    if (encoder_markers_push(s, dct, &ident))
        goto bail;

    if (JSON_Accu_Accumulate(state, rval, state->JSON_open_dict))
        goto bail;

    if (indented) {
        if (encoder_accumulate_newline_indent(s, state, rval, inner_indent_level))
            goto bail;
    }

    /* Fast path: when sort_keys is off and dct is an exact dict,
     * iterate with PyDict_Next to avoid allocating an items list.
     * Py_BEGIN_CRITICAL_SECTION prevents concurrent dict mutation
     * on free-threaded builds; on default builds it is a no-op. */
    if (s->item_sort_kw == Py_None && PyDict_CheckExact(dct)) {
        Py_ssize_t pos = 0;
        PyObject *key, *value;
        int err = 0;

        idx = 0;
        Py_BEGIN_CRITICAL_SECTION(dct);
        while (PyDict_Next(dct, &pos, &key, &value)) {
            Py_INCREF(key);
            Py_INCREF(value);

            encoded = encoder_encode_dict_key(s, key);
            Py_DECREF(key);
            if (encoded == NULL) {
                Py_DECREF(value); err = 1; break;
            }
            if (encoded == Py_None) {
                /* skipkeys */
                encoded = NULL;
                Py_DECREF(value);
                continue;
            }
            if (idx) {
                if (JSON_Accu_Accumulate(state, rval, s->item_separator)) {
                    Py_DECREF(value); err = 1; break;
                }
                if (indented && encoder_accumulate_newline_indent(
                                    s, state, rval, inner_indent_level)) {
                    Py_DECREF(value); err = 1; break;
                }
            }
            if (JSON_Accu_Accumulate(state, rval, encoded)) {
                Py_DECREF(value); err = 1; break;
            }
            Py_CLEAR(encoded);
            if (JSON_Accu_Accumulate(state, rval, s->key_separator)) {
                Py_DECREF(value); err = 1; break;
            }
            if (encoder_listencode_obj(s, rval, value, inner_indent_level)) {
#if PY_VERSION_HEX >= 0x030B0000
                encoder_annotate_exception(state,
                    "when serializing %s item %R",
                    Py_TYPE(dct)->tp_name, key);
#endif
                Py_DECREF(value); err = 1; break;
            }
            Py_DECREF(value);
            idx++;
        }
        Py_END_CRITICAL_SECTION();

        if (err || PyErr_Occurred())
            goto bail;
    }
    else {
        /* Slow path: sorted iteration, dict subclasses, or non-dict
         * mappings.  Build an items list via encoder_dict_iteritems. */
        iter = encoder_dict_iteritems(s, dct);
        if (iter == NULL)
            goto bail;

        idx = 0;
        while ((item = PyIter_Next(iter))) {
            PyObject *key, *value;
            if (!PyTuple_Check(item) || Py_SIZE(item) != 2) {
                PyErr_SetString(PyExc_ValueError, "items must return 2-tuples");
                goto bail;
            }
            key = PyTuple_GET_ITEM(item, 0);
            value = PyTuple_GET_ITEM(item, 1);

            encoded = encoder_encode_dict_key(s, key);
            if (encoded == NULL)
                goto bail;
            if (encoded == Py_None) {
                /* skipkeys */
                encoded = NULL;
                Py_CLEAR(item);
                continue;
            }
            if (idx) {
                if (JSON_Accu_Accumulate(state, rval, s->item_separator))
                    goto bail;
                if (indented && encoder_accumulate_newline_indent(
                                    s, state, rval, inner_indent_level))
                    goto bail;
            }
            if (JSON_Accu_Accumulate(state, rval, encoded))
                goto bail;
            Py_CLEAR(encoded);
            if (JSON_Accu_Accumulate(state, rval, s->key_separator))
                goto bail;
            if (encoder_listencode_obj(s, rval, value, inner_indent_level)) {
#if PY_VERSION_HEX >= 0x030B0000
                encoder_annotate_exception(state,
                    "when serializing %s item %R",
                    Py_TYPE(dct)->tp_name, key);
#endif
                goto bail;
            }
            Py_CLEAR(item);
            idx++;
        }
        Py_CLEAR(iter);
        if (PyErr_Occurred())
            goto bail;
    }

    if (encoder_markers_pop(s, ident))
        goto bail;
    ident = NULL;
    if (indented) {
        if (encoder_accumulate_newline_indent(s, state, rval, indent_level))
            goto bail;
    }
    if (JSON_Accu_Accumulate(state, rval, state->JSON_close_dict))
        goto bail;
    return 0;

bail:
    Py_XDECREF(encoded);
    Py_XDECREF(item);
    Py_XDECREF(iter);
    Py_XDECREF(ident);
    return -1;
}


static int
encoder_listencode_list(PyEncoderObject *s, JSON_Accu *rval, PyObject *seq, Py_ssize_t indent_level)
{
    /* Encode Python list seq to a JSON term */
    _speedups_state *state = get_speedups_state(s->module_ref);
    PyObject *ident = NULL;
    PyObject *iter = NULL;
    PyObject *obj = NULL;
    /* See encoder_listencode_dict: inter-item indent prefixes are
     * written piece-by-piece via encoder_accumulate_newline_indent,
     * no intermediate PyObject is materialized. */
    int indented = (s->indent != Py_None);
    Py_ssize_t inner_indent_level = indented ? indent_level + 1 : indent_level;
    Py_ssize_t i = 0;
    int is_exact_fast;

    /* Emptiness check: use direct size for exact types to skip the
     * __bool__/__len__ method dispatch of PyObject_IsTrue. */
    if (PyList_CheckExact(seq)) {
        if (PyList_GET_SIZE(seq) == 0)
            return JSON_Accu_Accumulate(state, rval, state->JSON_empty_array);
        is_exact_fast = 1;
    }
    else if (PyTuple_CheckExact(seq)) {
        if (PyTuple_GET_SIZE(seq) == 0)
            return JSON_Accu_Accumulate(state, rval, state->JSON_empty_array);
        is_exact_fast = 1;
    }
    else {
        /* For non-exact types (list/tuple subclasses or other iterables)
         * we cannot short-circuit on emptiness without consuming the
         * iterator. iterable_as_array in particular reaches us with a
         * bare iterator whose PyObject_IsTrue always returns 1, so the
         * emptiness is only discovered on the first PyIter_Next call
         * inside the slow path below. */
        is_exact_fast = 0;
    }

    if (encoder_markers_push(s, seq, &ident))
        goto bail;

    if (is_exact_fast) {
        /* Fast path: exact list or exact tuple — iterate by index to
         * avoid allocating an iterator object.  Known non-empty from
         * the size check above, so the opening bracket and the first
         * newline_indent can be emitted unconditionally.
         * Py_BEGIN_CRITICAL_SECTION prevents concurrent list mutation
         * on free-threaded builds (tuples are immutable so the lock is
         * uncontested).
         *
         * Uses a local `item` variable (not the outer `obj`) so that
         * the bail handler's Py_XDECREF(obj) stays a no-op for this
         * path. */
        PyObject *item;
        Py_ssize_t size;
        int is_list = PyList_CheckExact(seq);
        int err = 0;

        if (JSON_Accu_Accumulate(state, rval, state->JSON_open_array))
            goto bail;

        if (indented) {
            if (encoder_accumulate_newline_indent(s, state, rval, inner_indent_level))
                goto bail;
        }

        Py_BEGIN_CRITICAL_SECTION(seq);
        size = is_list ? PyList_GET_SIZE(seq) : PyTuple_GET_SIZE(seq);
        for (i = 0; i < size; i++) {
            item = is_list ? PyList_GET_ITEM(seq, i)
                           : PyTuple_GET_ITEM(seq, i);
            Py_INCREF(item);
            if (i) {
                if (JSON_Accu_Accumulate(state, rval, s->item_separator)) {
                    Py_DECREF(item); err = 1; break;
                }
                if (indented && encoder_accumulate_newline_indent(
                                    s, state, rval, inner_indent_level)) {
                    Py_DECREF(item); err = 1; break;
                }
            }
            if (encoder_listencode_obj(s, rval, item, inner_indent_level)) {
#if PY_VERSION_HEX >= 0x030B0000
                encoder_annotate_exception(state,
                    "when serializing %s item %zd",
                    Py_TYPE(seq)->tp_name, i);
#endif
                Py_DECREF(item); err = 1; break;
            }
            Py_DECREF(item);
        }
        Py_END_CRITICAL_SECTION();

        if (err)
            goto bail;

        if (indented) {
            if (encoder_accumulate_newline_indent(s, state, rval, indent_level))
                goto bail;
        }
        if (JSON_Accu_Accumulate(state, rval, state->JSON_close_array))
            goto bail;
    }
    else {
        /* Slow path: list/tuple subclasses or arbitrary iterables.
         * The opening '[' and newline_indent are deferred until we
         * know there is at least one item, so an empty iterable still
         * emits a compact "[]" under an indent= setting. */
        int emitted_open = 0;

        iter = PyObject_GetIter(seq);
        if (iter == NULL)
            goto bail;
        while ((obj = PyIter_Next(iter))) {
            if (!emitted_open) {
                if (JSON_Accu_Accumulate(state, rval, state->JSON_open_array))
                    goto bail;
                if (indented && encoder_accumulate_newline_indent(
                                    s, state, rval, inner_indent_level))
                    goto bail;
                emitted_open = 1;
            }
            else {
                if (JSON_Accu_Accumulate(state, rval, s->item_separator))
                    goto bail;
                if (indented && encoder_accumulate_newline_indent(
                                    s, state, rval, inner_indent_level))
                    goto bail;
            }
            if (encoder_listencode_obj(s, rval, obj, inner_indent_level)) {
#if PY_VERSION_HEX >= 0x030B0000
                encoder_annotate_exception(state,
                    "when serializing %s item %zd",
                    Py_TYPE(seq)->tp_name, i);
#endif
                goto bail;
            }
            i++;
            Py_CLEAR(obj);
        }
        Py_CLEAR(iter);
        if (PyErr_Occurred())
            goto bail;

        if (!emitted_open) {
            if (JSON_Accu_Accumulate(state, rval, state->JSON_empty_array))
                goto bail;
        }
        else {
            if (indented) {
                if (encoder_accumulate_newline_indent(s, state, rval, indent_level))
                    goto bail;
            }
            if (JSON_Accu_Accumulate(state, rval, state->JSON_close_array))
                goto bail;
        }
    }

    if (encoder_markers_pop(s, ident))
        goto bail;
    ident = NULL;
    return 0;

bail:
    Py_XDECREF(obj);
    Py_XDECREF(iter);
    Py_XDECREF(ident);
    return -1;
}

static void
encoder_dealloc(PyObject *self)
{
    /* bpo-31095: UnTrack is needed before calling any callbacks */
#if PY_VERSION_HEX >= 0x030D0000
    PyTypeObject *tp = Py_TYPE(self);
#endif
    PyObject_GC_UnTrack(self);
    encoder_clear(self);
    Py_TYPE(self)->tp_free(self);
#if PY_VERSION_HEX >= 0x030D0000
    Py_DECREF(tp);
#endif
}

static int
encoder_traverse(PyObject *self, visitproc visit, void *arg)
{
    PyEncoderObject *s = (PyEncoderObject *)self;
#if PY_VERSION_HEX >= 0x030D0000
    /* Heap types must visit their type for GC. */
    Py_VISIT(Py_TYPE(self));
#else
    assert(PyEncoder_Check(self));
#endif
#define JSON_VISIT_FIELD(f) Py_VISIT(s->f);
    JSON_ENCODER_OBJECT_FIELDS(JSON_VISIT_FIELD)
#undef JSON_VISIT_FIELD
    return 0;
}

static int
encoder_clear(PyObject *self)
{
    /* Deallocate Encoder */
    PyEncoderObject *s = (PyEncoderObject *)self;
#if PY_VERSION_HEX < 0x030D0000
    assert(PyEncoder_Check(self));
#endif
#define JSON_CLEAR_FIELD(f) Py_CLEAR(s->f);
    JSON_ENCODER_OBJECT_FIELDS(JSON_CLEAR_FIELD)
#undef JSON_CLEAR_FIELD
    return 0;
}

PyDoc_STRVAR(encoder_doc, "_iterencode(obj, _current_indent_level) -> iterable");

#if PY_VERSION_HEX >= 0x030D0000
/* Heap type slots and spec for Python 3.13+ */
static PyType_Slot PyEncoderType_slots[] = {
    {Py_tp_doc, (void *)encoder_doc},
    {Py_tp_dealloc, encoder_dealloc},
    {Py_tp_call, encoder_call},
    {Py_tp_traverse, encoder_traverse},
    {Py_tp_clear, encoder_clear},
    {Py_tp_members, encoder_members},
    {Py_tp_new, encoder_new},
    {0, NULL}
};

static PyType_Spec PyEncoderType_spec = {
    .name = "simplejson._speedups.Encoder",
    .basicsize = sizeof(PyEncoderObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = PyEncoderType_slots,
};
#else
static PyTypeObject PyEncoderType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "simplejson._speedups.Encoder",       /* tp_name */
    sizeof(PyEncoderObject), /* tp_basicsize */
    0,                    /* tp_itemsize */
    encoder_dealloc, /* tp_dealloc */
    0,                    /* tp_print */
    0,                    /* tp_getattr */
    0,                    /* tp_setattr */
    0,                    /* tp_compare */
    0,                    /* tp_repr */
    0,                    /* tp_as_number */
    0,                    /* tp_as_sequence */
    0,                    /* tp_as_mapping */
    0,                    /* tp_hash */
    encoder_call,         /* tp_call */
    0,                    /* tp_str */
    0,                    /* tp_getattro */
    0,                    /* tp_setattro */
    0,                    /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,   /* tp_flags */
    encoder_doc,          /* tp_doc */
    encoder_traverse,     /* tp_traverse */
    encoder_clear,        /* tp_clear */
    0,                    /* tp_richcompare */
    0,                    /* tp_weaklistoffset */
    0,                    /* tp_iter */
    0,                    /* tp_iternext */
    0,                    /* tp_methods */
    encoder_members,      /* tp_members */
    0,                    /* tp_getset */
    0,                    /* tp_base */
    0,                    /* tp_dict */
    0,                    /* tp_descr_get */
    0,                    /* tp_descr_set */
    0,                    /* tp_dictoffset */
    0,                    /* tp_init */
    0,                    /* tp_alloc */
    encoder_new,          /* tp_new */
    0,                    /* tp_free */
};
#endif

static PyMethodDef speedups_methods[] = {
    {"encode_basestring_ascii",
        (PyCFunction)py_encode_basestring_ascii,
        METH_O,
        pydoc_encode_basestring_ascii},
    {"encode_basestring",
        (PyCFunction)py_encode_basestring,
        METH_O,
        pydoc_encode_basestring},
    {"scanstring",
        (PyCFunction)py_scanstring,
        METH_VARARGS,
        pydoc_scanstring},
    {NULL, NULL, 0, NULL}
};

PyDoc_STRVAR(module_doc,
"simplejson speedups\n");

/* Clear every state field that init_speedups_state may populate.
 * Called at the start of init_speedups_state so that re-initialization
 * (e.g. importlib.reload on pre-3.13 where the static state lives for
 * the lifetime of the interpreter) releases the previous references
 * instead of leaking them. Type fields are NOT touched here: on 3.13+
 * module_exec creates the heap types before calling this function, and
 * on older versions the type fields hold borrowed pointers to static
 * PyTypeObjects that must not be cleared. */
static void
reset_speedups_state_constants(_speedups_state *state)
{
    Py_CLEAR(state->JSON_Infinity);
    Py_CLEAR(state->JSON_NegInfinity);
    Py_CLEAR(state->JSON_NaN);
    Py_CLEAR(state->JSON_EmptyUnicode);
#if PY_MAJOR_VERSION < 3
    Py_CLEAR(state->JSON_EmptyStr);
    Py_CLEAR(state->JSON_EmptyStr_join);
#endif
    Py_CLEAR(state->JSON_s_null);
    Py_CLEAR(state->JSON_s_true);
    Py_CLEAR(state->JSON_s_false);
    Py_CLEAR(state->JSON_open_dict);
    Py_CLEAR(state->JSON_close_dict);
    Py_CLEAR(state->JSON_empty_dict);
    Py_CLEAR(state->JSON_open_array);
    Py_CLEAR(state->JSON_close_array);
    Py_CLEAR(state->JSON_empty_array);
    Py_CLEAR(state->JSON_newline);
    Py_CLEAR(state->JSON_sortargs);
    Py_CLEAR(state->JSON_itemgetter0);
    Py_CLEAR(state->JSON_attr_for_json);
    Py_CLEAR(state->JSON_attr_asdict);
    Py_CLEAR(state->JSON_attr_sort);
    Py_CLEAR(state->JSON_attr_encoded_json);
    Py_CLEAR(state->JSON_attr_add_note);
    Py_CLEAR(state->RawJSONType);
    Py_CLEAR(state->JSONDecodeError);
}

/* Shared initializer for per-module state. Called from module_exec
   on Python 3 and from init_speedups on Python 2. Assumes the type
   fields in state have already been populated. */
static int
init_speedups_state(_speedups_state *state, PyObject *module)
{
    /* Release any prior values. A no-op on the first call (fields are
     * already NULL from per-module zeroed storage on 3.13+ or from the
     * static BSS on older versions); on reload this releases the
     * previous references to avoid a refcount leak. */
    reset_speedups_state_constants(state);

    state->JSON_NaN = JSON_InternFromString("NaN");
    if (state->JSON_NaN == NULL)
        return -1;
    state->JSON_Infinity = JSON_InternFromString("Infinity");
    if (state->JSON_Infinity == NULL)
        return -1;
    state->JSON_NegInfinity = JSON_InternFromString("-Infinity");
    if (state->JSON_NegInfinity == NULL)
        return -1;
#if PY_MAJOR_VERSION >= 3
    state->JSON_EmptyUnicode = PyUnicode_New(0, 127);
#else
    state->JSON_EmptyStr = PyString_FromString("");
    if (state->JSON_EmptyStr == NULL)
        return -1;
    state->JSON_EmptyStr_join = PyObject_GetAttrString(state->JSON_EmptyStr, "join");
    if (state->JSON_EmptyStr_join == NULL)
        return -1;
    state->JSON_EmptyUnicode = PyUnicode_FromUnicode(NULL, 0);
#endif
    if (state->JSON_EmptyUnicode == NULL)
        return -1;
    state->JSON_s_null = JSON_InternFromString("null");
    if (state->JSON_s_null == NULL)
        return -1;
    state->JSON_s_true = JSON_InternFromString("true");
    if (state->JSON_s_true == NULL)
        return -1;
    state->JSON_s_false = JSON_InternFromString("false");
    if (state->JSON_s_false == NULL)
        return -1;
    state->JSON_open_dict = JSON_InternFromString("{");
    if (state->JSON_open_dict == NULL)
        return -1;
    state->JSON_close_dict = JSON_InternFromString("}");
    if (state->JSON_close_dict == NULL)
        return -1;
    state->JSON_empty_dict = JSON_InternFromString("{}");
    if (state->JSON_empty_dict == NULL)
        return -1;
    state->JSON_open_array = JSON_InternFromString("[");
    if (state->JSON_open_array == NULL)
        return -1;
    state->JSON_close_array = JSON_InternFromString("]");
    if (state->JSON_close_array == NULL)
        return -1;
    state->JSON_empty_array = JSON_InternFromString("[]");
    if (state->JSON_empty_array == NULL)
        return -1;
    state->JSON_newline = JSON_InternFromString("\n");
    if (state->JSON_newline == NULL)
        return -1;
    state->JSON_sortargs = PyTuple_New(0);
    if (state->JSON_sortargs == NULL)
        return -1;

    state->RawJSONType = import_dependency("simplejson.raw_json", "RawJSON");
    if (state->RawJSONType == NULL)
        return -1;
    state->JSONDecodeError = import_dependency("simplejson.errors", "JSONDecodeError");
    if (state->JSONDecodeError == NULL)
        return -1;

    {
        PyObject *operator_mod = PyImport_ImportModule("operator");
        if (!operator_mod)
            return -1;
        state->JSON_itemgetter0 = PyObject_CallMethod(operator_mod, "itemgetter", "i", 0);
        Py_DECREF(operator_mod);
        if (!state->JSON_itemgetter0)
            return -1;
    }

    /* Interned attribute names used in encoder hot paths. */
    state->JSON_attr_for_json = JSON_InternFromString("for_json");
    if (state->JSON_attr_for_json == NULL)
        return -1;
    state->JSON_attr_asdict = JSON_InternFromString("_asdict");
    if (state->JSON_attr_asdict == NULL)
        return -1;
    state->JSON_attr_sort = JSON_InternFromString("sort");
    if (state->JSON_attr_sort == NULL)
        return -1;
    state->JSON_attr_encoded_json = JSON_InternFromString("encoded_json");
    if (state->JSON_attr_encoded_json == NULL)
        return -1;
    state->JSON_attr_add_note = JSON_InternFromString("add_note");
    if (state->JSON_attr_add_note == NULL)
        return -1;

    (void)module;
    return 0;
}

/* Multi-phase initialization (PEP 489) for Python 3.5+. On 3.13+ this
 * path creates heap types and allocates per-module state so that each
 * interpreter gets its own copy; on 3.5-3.12 the type fields just point
 * at the statically-allocated PyTypeObjects and state lives in the
 * single _speedups_static_state instance. Either way, module_exec does
 * the work and get_speedups_state() gives uniform access. */
static int
module_exec(PyObject *m)
{
    _speedups_state *state = get_speedups_state(m);

#if PY_VERSION_HEX >= 0x030D0000
    /* Create heap types from specs, bound to this module */
    state->PyScannerType = PyType_FromModuleAndSpec(m, &PyScannerType_spec, NULL);
    if (state->PyScannerType == NULL)
        return -1;
    state->PyEncoderType = PyType_FromModuleAndSpec(m, &PyEncoderType_spec, NULL);
    if (state->PyEncoderType == NULL)
        return -1;
#else
    if (PyType_Ready(&PyScannerType) < 0)
        return -1;
    if (PyType_Ready(&PyEncoderType) < 0)
        return -1;
    /* Static types are eternal, so these are borrowed pointers kept
     * in the state struct for layout uniformity with the 3.13+ path.
     * There is nothing to refcount and no GC tracking here. */
    state->PyScannerType = (PyObject *)&PyScannerType;
    state->PyEncoderType = (PyObject *)&PyEncoderType;
    /* Scanner/Encoder instance construction needs a borrowed reference
     * to the module to store in module_ref; capture it here, before
     * anything else that might trigger instance creation. */
    _speedups_module = m;
#endif

#if PY_VERSION_HEX >= 0x030A0000
    if (PyModule_AddObjectRef(m, "make_scanner", state->PyScannerType) < 0)
        return -1;
    if (PyModule_AddObjectRef(m, "make_encoder", state->PyEncoderType) < 0)
        return -1;
#else
    Py_INCREF(state->PyScannerType);
    if (PyModule_AddObject(m, "make_scanner", state->PyScannerType) < 0) {
        Py_DECREF(state->PyScannerType);
        return -1;
    }
    Py_INCREF(state->PyEncoderType);
    if (PyModule_AddObject(m, "make_encoder", state->PyEncoderType) < 0) {
        Py_DECREF(state->PyEncoderType);
        return -1;
    }
#endif

    return init_speedups_state(state, m);
}

#if PY_VERSION_HEX >= 0x030D0000
static int
speedups_traverse(PyObject *m, visitproc visit, void *arg)
{
    _speedups_state *state = get_speedups_state(m);
    Py_VISIT(state->PyScannerType);
    Py_VISIT(state->PyEncoderType);
    Py_VISIT(state->JSON_Infinity);
    Py_VISIT(state->JSON_NegInfinity);
    Py_VISIT(state->JSON_NaN);
    Py_VISIT(state->JSON_EmptyUnicode);
    Py_VISIT(state->JSON_s_null);
    Py_VISIT(state->JSON_s_true);
    Py_VISIT(state->JSON_s_false);
    Py_VISIT(state->JSON_open_dict);
    Py_VISIT(state->JSON_close_dict);
    Py_VISIT(state->JSON_empty_dict);
    Py_VISIT(state->JSON_open_array);
    Py_VISIT(state->JSON_close_array);
    Py_VISIT(state->JSON_empty_array);
    Py_VISIT(state->JSON_sortargs);
    Py_VISIT(state->JSON_itemgetter0);
    Py_VISIT(state->JSON_attr_for_json);
    Py_VISIT(state->JSON_attr_asdict);
    Py_VISIT(state->JSON_attr_sort);
    Py_VISIT(state->JSON_attr_encoded_json);
    Py_VISIT(state->RawJSONType);
    Py_VISIT(state->JSONDecodeError);
    return 0;
}

static int
speedups_clear(PyObject *m)
{
    _speedups_state *state = get_speedups_state(m);
    Py_CLEAR(state->PyScannerType);
    Py_CLEAR(state->PyEncoderType);
    reset_speedups_state_constants(state);
    return 0;
}
#endif /* PY_VERSION_HEX >= 0x030D0000 */

#if PY_MAJOR_VERSION >= 3
static PyModuleDef_Slot module_slots[] = {
    {Py_mod_exec, module_exec},
#if PY_VERSION_HEX >= 0x030D0000
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL}
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_speedups",        /* m_name */
    module_doc,         /* m_doc */
#if PY_VERSION_HEX >= 0x030D0000
    sizeof(_speedups_state), /* m_size */
#else
    0,                  /* m_size: no per-module state on <3.13 */
#endif
    speedups_methods,   /* m_methods */
    module_slots,       /* m_slots (multi-phase init, PEP 489) */
#if PY_VERSION_HEX >= 0x030D0000
    speedups_traverse,  /* m_traverse */
    speedups_clear,     /* m_clear */
#else
    NULL,               /* m_traverse */
    NULL,               /* m_clear */
#endif
    NULL,               /* m_free */
};
#endif

static PyObject *
import_dependency(const char *module_name, const char *attr_name)
{
    PyObject *rval;
    PyObject *module = PyImport_ImportModule(module_name);
    if (module == NULL)
        return NULL;
    rval = PyObject_GetAttrString(module, attr_name);
    Py_DECREF(module);
    return rval;
}

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit__speedups(void)
{
    /* Multi-phase init (PEP 489): Python runs module_exec via Py_mod_exec slot */
    return PyModuleDef_Init(&moduledef);
}
#else
/* Python 2.7: single-phase init via Py_InitModule3 */
void
init_speedups(void)
{
    _speedups_state *state = &_speedups_static_state;
    PyObject *m;

    if (PyType_Ready(&PyScannerType) < 0)
        return;
    if (PyType_Ready(&PyEncoderType) < 0)
        return;
    state->PyScannerType = (PyObject *)&PyScannerType;
    state->PyEncoderType = (PyObject *)&PyEncoderType;

    m = Py_InitModule3("_speedups", speedups_methods, module_doc);
    if (m == NULL)
        return;
    _speedups_module = m;  /* borrowed; sys.modules keeps it alive */

    Py_INCREF(state->PyScannerType);
    if (PyModule_AddObject(m, "make_scanner", state->PyScannerType) < 0) {
        Py_DECREF(state->PyScannerType);
        return;
    }
    Py_INCREF(state->PyEncoderType);
    if (PyModule_AddObject(m, "make_encoder", state->PyEncoderType) < 0) {
        Py_DECREF(state->PyEncoderType);
        return;
    }
    if (init_speedups_state(state, m) < 0)
        return;
}
#endif
