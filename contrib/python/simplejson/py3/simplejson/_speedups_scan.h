/*
 * _speedups_scan.h -- templated JSON scanner function bodies.
 *
 * This file is NOT a traditional header and must not be used as one.
 * It contains function *definitions* and is #included multiple times
 * from _speedups.c with different macro settings to generate both
 * the Py2 bytes (_str) and the universal unicode (_unicode) variants
 * of scan_once, _parse_object, _parse_array, and _match_number
 * without code duplication. The caller must #define the following
 * macros before each #include, and must wrap the inclusion with
 *
 *     #define JSON_SPEEDUPS_SCAN_INCLUDING 1
 *     #include "_speedups_scan.h"
 *     #undef  JSON_SPEEDUPS_SCAN_INCLUDING
 *
 * so that accidental inclusion from any other file (or any other
 * code path in _speedups.c) is caught at compile time. The
 * JSON_SPEEDUPS_SCAN_INCLUDING gate is the strong form of the
 * sanity check; missing JSON_SCAN_SUFFIX is a weaker symptom and is
 * also diagnosed below.
 *
 * Expected macros (must be defined before each #include):
 *
 *   JSON_SCAN_SUFFIX                - Either _str or _unicode
 *   JSON_SCAN_DATA_INIT(pystr)      - Statements to set up `str` (data
 *                                     pointer) and `end_idx` locals
 *   JSON_SCAN_READ(idx)             - Read char at idx, returns JSON_UNICHR
 *   JSON_SCAN_SCANSTRING_CALL(...)  - scanstring_* call with the right args
 *   JSON_SCAN_NUMSTR_CREATE(s, e)   - Create a PyObject holding the numeric
 *                                     substring from start..end
 *   JSON_SCAN_PARSE_FLOAT_FAST(ns)  - Fast-path float parse (or fallback)
 *   JSON_SCAN_PARSE_INT_FAST(ns)    - Fast-path int parse (or fallback)
 *
 * The macros are #undef'd at the bottom of the file so the caller can
 * redefine them for the next #include.
 *
 * Example:
 *
 *   #define JSON_SCAN_SUFFIX _unicode
 *   #define JSON_SCAN_DATA_INIT(p) \
 *       PY2_UNUSED int kind = PyUnicode_KIND(p); \
 *       void *str = PyUnicode_DATA(p); \
 *       Py_ssize_t end_idx = PyUnicode_GET_LENGTH(p) - 1
 *   #define JSON_SCAN_READ(i) PyUnicode_READ(kind, str, (i))
 *   ...
 *   #define JSON_SPEEDUPS_SCAN_INCLUDING 1
 *   #include "_speedups_scan.h"
 *   #undef  JSON_SPEEDUPS_SCAN_INCLUDING
 */

#ifndef JSON_SPEEDUPS_SCAN_INCLUDING
#error "_speedups_scan.h must only be included by _speedups.c. See the header comment."
#endif

#ifndef JSON_SCAN_SUFFIX
#error "JSON_SCAN_SUFFIX must be defined before including _speedups_scan.h"
#endif

#define JSON_SCAN_CONCAT_(a, b) a##b
#define JSON_SCAN_CONCAT(a, b) JSON_SCAN_CONCAT_(a, b)
#define JSON_SCAN_FN(base) JSON_SCAN_CONCAT(base, JSON_SCAN_SUFFIX)

/* Advance idx past any JSON whitespace (space, tab, CR, LF). The
 * idx <= end_idx guard matches the parser's convention that end_idx
 * is the index of the LAST valid byte (i.e. length - 1). */
#define SKIP_WHITESPACE() \
    do { \
        while (idx <= end_idx && IS_WHITESPACE(JSON_SCAN_READ(idx))) \
            idx++; \
    } while (0)

static PyObject *
JSON_SCAN_FN(_match_number)(PyScannerObject *s, PyObject *pystr,
                            Py_ssize_t start, Py_ssize_t *next_idx_ptr)
{
    /* Read a JSON number from pystr.
       *next_idx_ptr is a return-by-reference index to the first character
       after the number. Returns a new PyObject representation of that
       number: PyInt/PyLong or PyFloat, or whatever parse_int/parse_float
       return if those are set. */
    _speedups_state *state = get_speedups_state(s->module_ref);
    JSON_SCAN_DATA_INIT(pystr);
    Py_ssize_t idx = start;
    int is_float = 0;
    JSON_UNICHR c;
    PyObject *rval;
    PyObject *numstr;

    /* read a sign if it's there, make sure it's not the end of the string */
    if (JSON_SCAN_READ(idx) == '-') {
        if (idx >= end_idx) {
            raise_errmsg(state, ERR_EXPECTING_VALUE, pystr, start);
            return NULL;
        }
        idx++;
    }

    /* read as many integer digits as we find as long as it doesn't start with 0 */
    c = JSON_SCAN_READ(idx);
    if (c == '0') {
        /* if it starts with 0 we only expect one integer digit */
        idx++;
    }
    else if (IS_DIGIT(c)) {
        idx++;
        while (idx <= end_idx && IS_DIGIT(JSON_SCAN_READ(idx))) {
            idx++;
        }
    }
    else {
        /* no integer digits, error */
        raise_errmsg(state, ERR_EXPECTING_VALUE, pystr, start);
        return NULL;
    }

    /* if the next char is '.' followed by a digit then read all float digits */
    if (idx < end_idx &&
        JSON_SCAN_READ(idx) == '.' &&
        IS_DIGIT(JSON_SCAN_READ(idx + 1))) {
        is_float = 1;
        idx += 2;
        while (idx <= end_idx && IS_DIGIT(JSON_SCAN_READ(idx))) idx++;
    }

    /* if the next char is 'e' or 'E' then maybe read the exponent (or backtrack) */
    if (idx < end_idx &&
        (JSON_SCAN_READ(idx) == 'e' || JSON_SCAN_READ(idx) == 'E')) {
        Py_ssize_t e_start = idx;
        idx++;

        /* read an exponent sign if present */
        if (idx < end_idx &&
            (JSON_SCAN_READ(idx) == '-' || JSON_SCAN_READ(idx) == '+')) idx++;

        /* read all digits */
        while (idx <= end_idx && IS_DIGIT(JSON_SCAN_READ(idx))) idx++;

        /* if we got a digit, then parse as float. if not, backtrack */
        if (IS_DIGIT(JSON_SCAN_READ(idx - 1))) {
            is_float = 1;
        }
        else {
            idx = e_start;
        }
    }

    /* copy the section we determined to be a number */
    numstr = JSON_SCAN_NUMSTR_CREATE(start, idx);
    if (numstr == NULL)
        return NULL;
    if (is_float) {
        /* parse as a float using a fast path if available,
           otherwise call user-defined method */
        if (s->parse_float != (PyObject *)&PyFloat_Type) {
            rval = PyObject_CallOneArg(s->parse_float, numstr);
        }
        else {
            rval = JSON_SCAN_PARSE_FLOAT_FAST(numstr);
        }
    }
    else {
        /* parse as an int using a fast path if available,
           otherwise call user-defined method */
        rval = JSON_SCAN_PARSE_INT_FAST(numstr);
    }
    Py_DECREF(numstr);
    *next_idx_ptr = idx;
    return rval;
}

static PyObject *
JSON_SCAN_FN(_parse_object)(PyScannerObject *s, PyObject *pystr,
                            Py_ssize_t idx, Py_ssize_t *next_idx_ptr)
{
    /* Read a JSON object from pystr.
       idx is the index of the first character after the opening curly brace.
       *next_idx_ptr is a return-by-reference index to the first character
       after the closing curly brace. */
    _speedups_state *state = get_speedups_state(s->module_ref);
    JSON_SCAN_DATA_INIT(pystr);
    PyObject *rval = NULL;
    PyObject *pairs = NULL;
    PyObject *item;
    PyObject *key = NULL;
    PyObject *val = NULL;
    int has_pairs_hook = (s->pairs_hook != Py_None);
    int did_parse = 0;
    Py_ssize_t next_idx;

    if (has_pairs_hook) {
        pairs = PyList_New(0);
        if (pairs == NULL)
            return NULL;
    }
    else {
        rval = PyDict_New();
        if (rval == NULL)
            return NULL;
    }

    /* skip whitespace after { */
    SKIP_WHITESPACE();

    /* only loop if the object is non-empty */
    if (idx <= end_idx && JSON_SCAN_READ(idx) != '}') {
        int trailing_delimiter = 0;
        while (idx <= end_idx) {
            trailing_delimiter = 0;

            /* read key */
            if (JSON_SCAN_READ(idx) != '"') {
                raise_errmsg(state, ERR_OBJECT_PROPERTY, pystr, idx);
                goto bail;
            }
            key = JSON_SCAN_SCANSTRING_CALL(idx + 1, &next_idx);
            if (key == NULL)
                goto bail;
            /* Intern the key through s->memo so repeated key strings
             * share one PyObject across this decode. Using SetDefault
             * collapses what used to be separate Get/Set lookups into
             * a single atomic call. */
            if (json_memo_intern_key(s->memo, &key) < 0)
                goto bail;
            idx = next_idx;

            /* skip whitespace between key and : delimiter, read :, skip
               whitespace */
            SKIP_WHITESPACE();
            if (idx > end_idx || JSON_SCAN_READ(idx) != ':') {
                raise_errmsg(state, ERR_OBJECT_PROPERTY_DELIMITER, pystr, idx);
                goto bail;
            }
            idx++;
            SKIP_WHITESPACE();

            /* read any JSON term */
            val = JSON_SCAN_FN(scan_once)(s, pystr, idx, &next_idx);
            if (val == NULL)
                goto bail;

            if (has_pairs_hook) {
                item = PyTuple_Pack(2, key, val);
                if (item == NULL)
                    goto bail;
                Py_CLEAR(key);
                Py_CLEAR(val);
                if (PyList_Append(pairs, item) == -1) {
                    Py_DECREF(item);
                    goto bail;
                }
                Py_DECREF(item);
            }
            else {
                if (PyDict_SetItem(rval, key, val) < 0)
                    goto bail;
                Py_CLEAR(key);
                Py_CLEAR(val);
            }
            idx = next_idx;

            /* skip whitespace before } or , */
            SKIP_WHITESPACE();

            /* bail if the object is closed or we didn't get the , delimiter */
            did_parse = 1;
            if (idx > end_idx) break;
            if (JSON_SCAN_READ(idx) == '}') {
                break;
            }
            else if (JSON_SCAN_READ(idx) != ',') {
                raise_errmsg(state, ERR_OBJECT_DELIMITER, pystr, idx);
                goto bail;
            }
            idx++;

            /* skip whitespace after , delimiter */
            SKIP_WHITESPACE();
            trailing_delimiter = 1;

            /* check for trailing comma before } */
            if (idx <= end_idx && JSON_SCAN_READ(idx) == '}') {
                raise_errmsg(state, ERR_TRAILING_COMMA_OBJECT, pystr, idx);
                goto bail;
            }
        }
        if (trailing_delimiter) {
            /* Truncated input after comma (e.g. '{"a":1,') */
            raise_errmsg(state, ERR_OBJECT_PROPERTY, pystr, idx);
            goto bail;
        }
    }

    /* verify that idx < end_idx, str[idx] should be '}' */
    if (idx > end_idx || JSON_SCAN_READ(idx) != '}') {
        if (did_parse) {
            raise_errmsg(state, ERR_OBJECT_DELIMITER, pystr, idx);
        } else {
            raise_errmsg(state, ERR_OBJECT_PROPERTY_FIRST, pystr, idx);
        }
        goto bail;
    }

    /* if pairs_hook is not None: rval = object_pairs_hook(pairs) */
    if (s->pairs_hook != Py_None) {
        val = PyObject_CallOneArg(s->pairs_hook, pairs);
        if (val == NULL)
            goto bail;
        Py_DECREF(pairs);
        *next_idx_ptr = idx + 1;
        return val;
    }

    /* if object_hook is not None: rval = object_hook(rval) */
    if (s->object_hook != Py_None) {
        val = PyObject_CallOneArg(s->object_hook, rval);
        if (val == NULL)
            goto bail;
        Py_DECREF(rval);
        rval = val;
        val = NULL;
    }
    *next_idx_ptr = idx + 1;
    return rval;
bail:
    Py_XDECREF(rval);
    Py_XDECREF(key);
    Py_XDECREF(val);
    Py_XDECREF(pairs);
    return NULL;
}

static PyObject *
JSON_SCAN_FN(_parse_array)(PyScannerObject *s, PyObject *pystr,
                           Py_ssize_t idx, Py_ssize_t *next_idx_ptr)
{
    /* Read a JSON array from pystr.
       idx is the index of the first character after the opening brace.
       *next_idx_ptr is a return-by-reference index to the first character
       after the closing brace. */
    _speedups_state *state = get_speedups_state(s->module_ref);
    JSON_SCAN_DATA_INIT(pystr);
    PyObject *val = NULL;
    PyObject *rval = PyList_New(0);
    Py_ssize_t next_idx;
    if (rval == NULL)
        return NULL;

    /* skip whitespace after [ */
    SKIP_WHITESPACE();

    /* only loop if the array is non-empty */
    if (idx <= end_idx && JSON_SCAN_READ(idx) != ']') {
        int trailing_delimiter = 0;
        while (idx <= end_idx) {
            trailing_delimiter = 0;
            /* read any JSON term and de-tuplefy the (rval, idx) */
            val = JSON_SCAN_FN(scan_once)(s, pystr, idx, &next_idx);
            if (val == NULL) {
                goto bail;
            }

            if (PyList_Append(rval, val) == -1)
                goto bail;

            Py_CLEAR(val);
            idx = next_idx;

            /* skip whitespace between term and , */
            SKIP_WHITESPACE();

            /* bail if the array is closed or we didn't get the , delimiter */
            if (idx > end_idx) break;
            if (JSON_SCAN_READ(idx) == ']') {
                break;
            }
            else if (JSON_SCAN_READ(idx) != ',') {
                raise_errmsg(state, ERR_ARRAY_DELIMITER, pystr, idx);
                goto bail;
            }
            idx++;

            /* skip whitespace after , */
            SKIP_WHITESPACE();
            trailing_delimiter = 1;

            /* check for trailing comma before ] */
            if (idx <= end_idx && JSON_SCAN_READ(idx) == ']') {
                raise_errmsg(state, ERR_TRAILING_COMMA_ARRAY, pystr, idx);
                goto bail;
            }
        }
        if (trailing_delimiter) {
            /* Truncated input after comma (e.g. "[42,") */
            raise_errmsg(state, ERR_EXPECTING_VALUE, pystr, idx);
            goto bail;
        }
    }

    /* verify that idx < end_idx, str[idx] should be ']' */
    if (idx > end_idx || JSON_SCAN_READ(idx) != ']') {
        if (PyList_GET_SIZE(rval)) {
            raise_errmsg(state, ERR_ARRAY_DELIMITER, pystr, idx);
        } else {
            raise_errmsg(state, ERR_ARRAY_VALUE_FIRST, pystr, idx);
        }
        goto bail;
    }
    /* apply array_hook if set */
    if (s->array_hook != Py_None) {
        val = PyObject_CallOneArg(s->array_hook, rval);
        if (val == NULL)
            goto bail;
        Py_DECREF(rval);
        rval = val;
        val = NULL;
    }
    *next_idx_ptr = idx + 1;
    return rval;
bail:
    Py_XDECREF(val);
    Py_DECREF(rval);
    return NULL;
}

static PyObject *
JSON_SCAN_FN(scan_once)(PyScannerObject *s, PyObject *pystr,
                        Py_ssize_t idx, Py_ssize_t *next_idx_ptr)
{
    /* Read one JSON term (of any kind) from pystr.
       idx is the index of the first character of the term.
       *next_idx_ptr is a return-by-reference index to the first character
       after the term. */
    _speedups_state *state = get_speedups_state(s->module_ref);
    JSON_SCAN_DATA_INIT(pystr);
    Py_ssize_t length = end_idx + 1;
    PyObject *rval = NULL;
    int fallthrough = 0;
    if (idx < 0 || idx >= length) {
        raise_errmsg(state, ERR_EXPECTING_VALUE, pystr, idx);
        return NULL;
    }
    switch (JSON_SCAN_READ(idx)) {
        case '"':
            /* string */
            rval = JSON_SCAN_SCANSTRING_CALL(idx + 1, next_idx_ptr);
            break;
        case '{':
            /* object */
            if (Py_EnterRecursiveCall(" while decoding a JSON object "
                                      "from a string"))
                return NULL;
            rval = JSON_SCAN_FN(_parse_object)(s, pystr, idx + 1, next_idx_ptr);
            Py_LeaveRecursiveCall();
            break;
        case '[':
            /* array */
            if (Py_EnterRecursiveCall(" while decoding a JSON array "
                                      "from a string"))
                return NULL;
            rval = JSON_SCAN_FN(_parse_array)(s, pystr, idx + 1, next_idx_ptr);
            Py_LeaveRecursiveCall();
            break;
        case 'n':
            /* null */
            if ((idx + 3 < length)
                && JSON_SCAN_READ(idx + 1) == 'u'
                && JSON_SCAN_READ(idx + 2) == 'l'
                && JSON_SCAN_READ(idx + 3) == 'l') {
                Py_INCREF(Py_None);
                *next_idx_ptr = idx + 4;
                rval = Py_None;
            }
            else
                fallthrough = 1;
            break;
        case 't':
            /* true */
            if ((idx + 3 < length)
                && JSON_SCAN_READ(idx + 1) == 'r'
                && JSON_SCAN_READ(idx + 2) == 'u'
                && JSON_SCAN_READ(idx + 3) == 'e') {
                Py_INCREF(Py_True);
                *next_idx_ptr = idx + 4;
                rval = Py_True;
            }
            else
                fallthrough = 1;
            break;
        case 'f':
            /* false */
            if ((idx + 4 < length)
                && JSON_SCAN_READ(idx + 1) == 'a'
                && JSON_SCAN_READ(idx + 2) == 'l'
                && JSON_SCAN_READ(idx + 3) == 's'
                && JSON_SCAN_READ(idx + 4) == 'e') {
                Py_INCREF(Py_False);
                *next_idx_ptr = idx + 5;
                rval = Py_False;
            }
            else
                fallthrough = 1;
            break;
        case 'N':
            /* NaN */
            if ((idx + 2 < length)
                && JSON_SCAN_READ(idx + 1) == 'a'
                && JSON_SCAN_READ(idx + 2) == 'N') {
                rval = _parse_constant(s, pystr, state->JSON_NaN, idx, next_idx_ptr);
            }
            else
                fallthrough = 1;
            break;
        case 'I':
            /* Infinity */
            if ((idx + 7 < length)
                && JSON_SCAN_READ(idx + 1) == 'n'
                && JSON_SCAN_READ(idx + 2) == 'f'
                && JSON_SCAN_READ(idx + 3) == 'i'
                && JSON_SCAN_READ(idx + 4) == 'n'
                && JSON_SCAN_READ(idx + 5) == 'i'
                && JSON_SCAN_READ(idx + 6) == 't'
                && JSON_SCAN_READ(idx + 7) == 'y') {
                rval = _parse_constant(s, pystr, state->JSON_Infinity, idx, next_idx_ptr);
            }
            else
                fallthrough = 1;
            break;
        case '-':
            /* -Infinity */
            if ((idx + 8 < length)
                && JSON_SCAN_READ(idx + 1) == 'I'
                && JSON_SCAN_READ(idx + 2) == 'n'
                && JSON_SCAN_READ(idx + 3) == 'f'
                && JSON_SCAN_READ(idx + 4) == 'i'
                && JSON_SCAN_READ(idx + 5) == 'n'
                && JSON_SCAN_READ(idx + 6) == 'i'
                && JSON_SCAN_READ(idx + 7) == 't'
                && JSON_SCAN_READ(idx + 8) == 'y') {
                rval = _parse_constant(s, pystr, state->JSON_NegInfinity, idx, next_idx_ptr);
            }
            else
                fallthrough = 1;
            break;
        default:
            fallthrough = 1;
    }
    /* Didn't find a string, object, array, or named constant.
       Look for a number. */
    if (fallthrough)
        rval = JSON_SCAN_FN(_match_number)(s, pystr, idx, next_idx_ptr);
    return rval;
}

#undef JSON_SCAN_FN
#undef JSON_SCAN_CONCAT
#undef JSON_SCAN_CONCAT_
#undef JSON_SCAN_SUFFIX
#undef JSON_SCAN_DATA_INIT
#undef JSON_SCAN_READ
#undef JSON_SCAN_SCANSTRING_CALL
#undef JSON_SCAN_NUMSTR_CREATE
#undef JSON_SCAN_PARSE_FLOAT_FAST
#undef JSON_SCAN_PARSE_INT_FAST
#undef SKIP_WHITESPACE
