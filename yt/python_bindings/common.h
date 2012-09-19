#pragma once

#include <util/generic/stroka.h>

#include <contrib/libs/pycxx/Objects.hxx>

// Unfortunately PyCxx does not implement some usefull methods.
namespace Py {

inline bool IsInstance(const Object& obj, const Object& cls)
{
    return PyObject_IsInstance(*obj, *cls) == 1;
}

inline bool IsStringLike(const Object& obj)
{
    return IsInstance(obj, String().type());
}

inline String ConvertToString(const Object& obj)
{
    return String(PyObject_Str(*obj));
}

inline Object GetAttr(const Object& obj, const std::string& fieldName) {
    if (!obj.hasAttr(fieldName)) {
        throw RuntimeError("There is no field " + fieldName);
    }
    return obj.getAttr(fieldName);
}

} // namespace Py



namespace NYT {

inline Stroka AsStroka(const Py::String& pyString)
{
    std::string string(pyString);
    Stroka res(string.length());
    std::copy(string.begin(), string.end(), res.begin());
    return res;
}

inline Py::String ConvertToPythonString(const Stroka& string)
{
    return Py::String(string.c_str(), string.length());
}


} // namespace NYT
