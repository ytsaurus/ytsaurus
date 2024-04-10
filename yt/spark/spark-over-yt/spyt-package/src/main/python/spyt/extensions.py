from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.column import _to_java_column
from pyspark.cloudpickle.cloudpickle import _extract_code_globals_cache, GLOBAL_OPS
import dis
import sys
import types


# DataFrameReader extensions
def read_yt(self, *paths):
    def fix_path(path):
        return path[1:] if path.startswith("//") else path
    return self.format("yt").load(path=[fix_path(path) for path in paths])


def _dict_to_struct(dict_type):
    return StructType([StructField(name, data_type) for (name, data_type) in dict_type.items()])


def schema_hint(self, fields):
    spark = SparkSession.builder.getOrCreate()

    struct_fields = []
    for name, data_type in fields.items():
        if isinstance(data_type, dict):
            data_type = _dict_to_struct(data_type)
        field = StructField(name, data_type)
        struct_fields.append(field)
    schema = StructType(struct_fields)

    jschema = spark._jsparkSession.parseDataType(schema.json())
    self._jreader = spark._jvm.tech.ytsaurus.spyt.PythonUtils.schemaHint(self._jreader, jschema)
    return self


# DataFrameWriter extensions
def write_yt(self, path, mode=None):
    def fix_path(path):
        return path[1:] if path.startswith("//") else path
    self.mode(mode)
    self.format("yt").save(fix_path(path))


def sorted_by(self, *cols, **kwargs):
    unique_keys = kwargs.get("unique_keys") or False
    import json
    return self.option("sort_columns", json.dumps(cols)).option("unique_keys", str(unique_keys))


def optimize_for(self, optimize_mode):
    return self.option("optimize_for", optimize_mode)


# DataFrame extensions
def withYsonColumn(self, colName, col):
    java_column = _to_java_column(col)
    return DataFrame(
        self._sc._jvm.tech.ytsaurus.spyt.PythonUtils.withYsonColumn(self._jdf, colName, java_column),
        self.sql_ctx
    )


def transform(self, func):
    return func(self)


# Cloudpickle extensions. Must be applied if Spark version is less than 3.4.0
def _extract_code_globals(co):
    """
    Find all globals names read or written to by codeblock co
    """
    out_names = _extract_code_globals_cache.get(co)
    if out_names is None:
        # SPYT-415: From Spark 3.4
        if sys.version_info < (3, 11):
            names = co.co_names
            out_names = {names[oparg] for _, oparg in _walk_global_ops(co)}
        else:
            # We use a dict with None values instead of a set to get a
            # deterministic order (assuming Python 3.6+) and avoid introducing
            # non-deterministic pickle bytes as a results.
            out_names = {name: None for name in _walk_global_ops(co)}

        # Declaring a function inside another one using the "def ..."
        # syntax generates a constant code object corresonding to the one
        # of the nested function's As the nested function may itself need
        # global variables, we need to introspect its code, extract its
        # globals, (look for code object in it's co_consts attribute..) and
        # add the result to code_globals
        if co.co_consts:
            for const in co.co_consts:
                if isinstance(const, types.CodeType):
                    out_names.update(_extract_code_globals(const))

        _extract_code_globals_cache[co] = out_names

    return out_names


# SPYT-415: From Spark 3.4
if sys.version_info < (3, 11):
    def _walk_global_ops(code):
        """
        Yield (opcode, argument number) tuples for all
        global-referencing instructions in *code*.
        """
        for instr in dis.get_instructions(code):
            op = instr.opcode
            if op in GLOBAL_OPS:
                yield op, instr.arg
else:
    def _walk_global_ops(code):
        """
        Yield referenced name for all global-referencing instructions in *code*.
        """
        for instr in dis.get_instructions(code):
            op = instr.opcode
            if op in GLOBAL_OPS:
                yield instr.argval


def _code_reduce(obj):
    """codeobject reducer"""
    # SPYT-415: From Spark 3.4
    # If you are not sure about the order of arguments, take a look at help
    # of the specific type from types, for example:
    # >>> from types import CodeType
    # >>> help(CodeType)
    if hasattr(obj, "co_exceptiontable"):  # pragma: no branch
        # Python 3.11 and later: there are some new attributes
        # related to the enhanced exceptions.
        args = (
            obj.co_argcount, obj.co_posonlyargcount,
            obj.co_kwonlyargcount, obj.co_nlocals, obj.co_stacksize,
            obj.co_flags, obj.co_code, obj.co_consts, obj.co_names,
            obj.co_varnames, obj.co_filename, obj.co_name, obj.co_qualname,
            obj.co_firstlineno, obj.co_linetable, obj.co_exceptiontable,
            obj.co_freevars, obj.co_cellvars,
        )
    elif hasattr(obj, "co_linetable"):  # pragma: no branch
        # Python 3.10 and later: obj.co_lnotab is deprecated and constructor
        # expects obj.co_linetable instead.
        args = (
            obj.co_argcount, obj.co_posonlyargcount,
            obj.co_kwonlyargcount, obj.co_nlocals, obj.co_stacksize,
            obj.co_flags, obj.co_code, obj.co_consts, obj.co_names,
            obj.co_varnames, obj.co_filename, obj.co_name,
            obj.co_firstlineno, obj.co_linetable, obj.co_freevars,
            obj.co_cellvars
        )
    elif hasattr(obj, "co_nmeta"):  # pragma: no cover
        # "nogil" Python: modified attributes from 3.9
        args = (
            obj.co_argcount, obj.co_posonlyargcount,
            obj.co_kwonlyargcount, obj.co_nlocals, obj.co_framesize,
            obj.co_ndefaultargs, obj.co_nmeta,
            obj.co_flags, obj.co_code, obj.co_consts,
            obj.co_varnames, obj.co_filename, obj.co_name,
            obj.co_firstlineno, obj.co_lnotab, obj.co_exc_handlers,
            obj.co_jump_table, obj.co_freevars, obj.co_cellvars,
            obj.co_free2reg, obj.co_cell2reg
        )
    elif hasattr(obj, "co_posonlyargcount"):
        # Backward compat for 3.9 and older
        args = (
            obj.co_argcount, obj.co_posonlyargcount,
            obj.co_kwonlyargcount, obj.co_nlocals, obj.co_stacksize,
            obj.co_flags, obj.co_code, obj.co_consts, obj.co_names,
            obj.co_varnames, obj.co_filename, obj.co_name,
            obj.co_firstlineno, obj.co_lnotab, obj.co_freevars,
            obj.co_cellvars
        )
    else:
        # Backward compat for even older versions of Python
        args = (
            obj.co_argcount, obj.co_kwonlyargcount, obj.co_nlocals,
            obj.co_stacksize, obj.co_flags, obj.co_code, obj.co_consts,
            obj.co_names, obj.co_varnames, obj.co_filename,
            obj.co_name, obj.co_firstlineno, obj.co_lnotab,
            obj.co_freevars, obj.co_cellvars
        )
    return types.CodeType, args
