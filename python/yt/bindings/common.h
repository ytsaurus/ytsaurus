#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/stroka.h>

#include <contrib/libs/pycxx/Objects.hxx>

// Unfortunately PyCxx does not implement some useful methods.
namespace Py {

inline bool IsInstance(const Object& obj, const Object& cls)
{
    return PyObject_IsInstance(*obj, *cls) == 1;
}

inline bool IsStringLike(const Object& obj)
{
    return PyString_Check(obj.ptr());
}

inline String ConvertToString(const Object& obj)
{
    return String(PyObject_Str(*obj), true);
}

inline Object GetAttr(const Object& obj, const std::string& fieldName) {
    if (!obj.hasAttr(fieldName)) {
        throw RuntimeError("There is no field " + fieldName);
    }
    return obj.getAttr(fieldName);
}

} // namespace Py


namespace NYT {

inline TStringBuf ConvertToStringBuf(const Py::String& pyString)
{
    char* stringData;
    Py_ssize_t length;
    PyString_AsStringAndSize(pyString.ptr(), &stringData, &length);
    return TStringBuf(stringData, length);
}

inline Stroka ConvertToStroka(const Py::String& pyString)
{
    char* stringData;
    Py_ssize_t length;
    PyString_AsStringAndSize(pyString.ptr(), &stringData, &length);
    return Stroka(stringData, length);
}

inline Py::String ConvertToPythonString(const Stroka& string)
{
    return Py::String(string.c_str(), string.length());
}

namespace NPython {

Py::Object ExtractArgument(Py::Tuple& args, Py::Dict& kwargs, const std::string& name);

bool HasArgument(Py::Tuple& args, Py::Dict& kwargs, const std::string& name);

extern Py::Object YsonError;

} // namespace NPython

} // namespace NYT
