#pragma once

#include "public.h"

#include <yt/yt/python/common/helpers.h>

#include <library/cpp/skiff/public.h>

#include <CXX/Objects.hxx> // pycxx

#include <functional>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 ConsecutiveRowIndex = -1;
constexpr i64 ControlAttributeNotAvailable = -2;

struct TSkiffRowContext
{
    int TableIndex = 0;
    bool KeySwitch = false;
    i64 RowIndex = ConsecutiveRowIndex;
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
bool IsPySchemaHasRuntimeValidation(Py::Object pySchema);
PyObjectPtr GetSchemaType(const std::string& name);
EPythonType GetPythonType(Py::Object pyType);
std::string GetRowClassName(Py::Object pySchema);

////////////////////////////////////////////////////////////////////////////////

inline const std::string WireTypeFieldName = "_wire_type";
inline const std::string PyTypeFieldName = "_py_type";
inline const std::string PyWireTypeFieldName = "_py_wire_type";
inline const std::string FieldsFieldName = "_fields";
inline const std::string PySchemaFieldName = "_py_schema";
inline const std::string NameFieldName = "_name";
inline const std::string ItemFieldName = "_item";
inline const std::string ElementsFieldName = "_elements";
inline const std::string KeyFieldName = "_key";
inline const std::string ValueFieldName = "_value";
inline const std::string StructSchemaFieldName = "_struct_schema";
inline const std::string OtherColumnsFieldFieldName = "_other_columns_field";
inline const std::string ControlAttributesFieldName = "_control_attributes";
inline const std::string SystemColumnsFieldName = "_SYSTEM_COLUMNS";
inline const std::string IsTiTypeOptionalFieldName = "_is_ti_type_optional";
inline const std::string SchemaRuntimeContextFieldName = "_schema_runtime_context";
inline const std::string ValidateOptionalOnRuntimeFieldName = "_validate_optional_on_runtime";
inline const std::string QualNameFieldName = "__qualname__";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
