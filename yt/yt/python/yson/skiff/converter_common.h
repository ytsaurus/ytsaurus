#pragma once

#include "public.h"

#include <yt/yt/python/common/helpers.h>

#include <library/cpp/skiff/public.h>

#include <Objects.hxx> // pycxx

#include <functional>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

struct TSkiffRowContext
{
    int TableIndex = 0;
    bool KeySwitch = false;
    i64 RowIndex = -1;
    i64 RangeIndex = 0;
};

DEFINE_ENUM(EPythonType,
    (Str)
    (Bytes)
    (Int)
    (Float)
    (Bool)
);

////////////////////////////////////////////////////////////////////////////////

using TRowSkiffToPythonConverter = std::function<PyObjectPtr(NSkiff::TCheckedInDebugSkiffParser*, TSkiffRowContext*)>;
using TSkiffToPythonConverter = std::function<PyObjectPtr(NSkiff::TCheckedInDebugSkiffParser*)>;
using TPythonToSkiffConverter = std::function<void(PyObject*, NSkiff::TCheckedInDebugSkiffWriter*)>;

////////////////////////////////////////////////////////////////////////////////

bool IsTiTypeOptional(Py::Object pySchema);
PyObjectPtr GetSchemaType(const TString& name);
EPythonType GetPythonType(Py::Object pyType);
TString GetRowClassName(Py::Object pySchema);

////////////////////////////////////////////////////////////////////////////////

const TString WireTypeFieldName = "_wire_type";
const TString PyTypeFieldName = "_py_type";
const TString FieldsFieldName = "_fields";
const TString PySchemaFieldName = "_py_schema";
const TString NameFieldName = "_name";
const TString ItemFieldName = "_item";
const TString KeyFieldName = "_key";
const TString ValueFieldName = "_value";
const TString StructSchemaFieldName = "_struct_schema";
const TString OtherColumnsFieldFieldName = "_other_columns_field";
const TString ControlAttributesFieldName = "_control_attributes";
const TString SystemColumnsFieldName = "_SYSTEM_COLUMNS";
const TString IsTiTypeOptionalFieldName = "_is_ti_type_optional";
const TString QualNameFieldName = "__qualname__";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
