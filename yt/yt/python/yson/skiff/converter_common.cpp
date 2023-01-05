#include "converter_common.h"

#include <yt/yt/python/common/helpers.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/enum.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

namespace NYT::NPython {

using namespace NSkiff;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

bool IsTiTypeOptional(Py::Object pySchema)
{
    return GetAttr(pySchema, IsTiTypeOptionalFieldName).as_bool();
}

PyObjectPtr GetSchemaType(const TString& name)
{
    return PyObjectPtr(GetModuleAttribute("yt.wrapper.schema.internal_schema", name));
}

EPythonType GetPythonType(Py::Object pyType)
{
    if (pyType.is(reinterpret_cast<PyObject*>(&PyLong_Type))) {
        return EPythonType::Int;
    } else if (pyType.is(reinterpret_cast<PyObject*>(&PyUnicode_Type))) {
        return EPythonType::Str;
    } else if (pyType.is(reinterpret_cast<PyObject*>(&PyBytes_Type))) {
        return EPythonType::Bytes;
    } else if (pyType.is(reinterpret_cast<PyObject*>(&PyFloat_Type))) {
        return EPythonType::Float;
    } else if (pyType.is(reinterpret_cast<PyObject*>(&PyBool_Type))) {
        return EPythonType::Bool;
    } else {
        THROW_ERROR_EXCEPTION("It's a bug, please contact yt@. Unexpected python type %Qlv",
            pyType.repr().as_string());
    }
}

TString GetRowClassName(Py::Object pySchema)
{
    auto pyType = GetAttr(GetAttr(pySchema, StructSchemaFieldName), PyTypeFieldName);
    return TString(GetAttr(pyType, QualNameFieldName).as_string());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
