#include "helpers.h"

namespace Py {

////////////////////////////////////////////////////////////////////////////////

bool IsInstance(const Object& obj, const Object& cls)
{
    return PyObject_IsInstance(*obj, *cls) == 1;
}

bool IsInteger(const Object& obj)
{
#if PY_MAJOR_VERSION < 3
    return PyInt_Check(obj.ptr()) || PyLong_Check(obj.ptr());
#else
    return PyLong_Check(obj.ptr());
#endif
}

bool IsFloat(const Object& obj)
{
    return PyFloat_Check(obj.ptr());
}

TStringBuf ConvertToStringBuf(const Bytes& pyString)
{
    char* stringData;
    Py_ssize_t length;
    PyBytes_AsStringAndSize(pyString.ptr(), &stringData, &length);
    return TStringBuf(stringData, length);
}

TString ConvertStringObjectToString(const Object& obj)
{
    Object pyString = obj;
    if (!PyBytes_Check(pyString.ptr())) {
        if (PyUnicode_Check(pyString.ptr())) {
            pyString = Py::Object(PyUnicode_AsUTF8String(pyString.ptr()), true);
        } else {
            throw RuntimeError("Object '" + Repr(pyString) + "' is not bytes or unicode string");
        }
    }
    char* stringData;
    Py_ssize_t length;
    PyBytes_AsStringAndSize(pyString.ptr(), &stringData, &length);
    return TString(stringData, length);
}

Bytes ConvertToPythonString(const TString& string)
{
    return Py::Bytes(string.c_str(), string.length());
}

i64 ConvertToLongLong(const Object& obj)
{
    return static_cast<i64>(Py::LongLong(obj));
}

Object GetAttr(const Object& obj, const std::string& fieldName)
{
    if (!obj.hasAttr(fieldName)) {
        throw RuntimeError("There is no field " + fieldName);
    }
    return obj.getAttr(fieldName);
}

std::string Repr(const Object& obj)
{
    return obj.repr().as_std_string("utf-8", "replace");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace Py

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object ExtractArgument(Py::Tuple& args, Py::Dict& kwargs, const std::string& name)
{
    Py::Object result;
    if (kwargs.hasKey(name)) {
        result = kwargs[name];
        kwargs.delItem(name);
    } else {
        if (args.length() == 0) {
            throw Py::RuntimeError("Missing argument '" + name + "'");
        }
        result = args.front();
        args = args.getSlice(1, args.length());
    }
    return result;
}

bool HasArgument(const Py::Tuple& args, const Py::Dict& kwargs, const std::string& name)
{
    if (kwargs.hasKey(name)) {
        return true;
    } else {
        return args.length() > 0;
    }
}

void ValidateArgumentsEmpty(const Py::Tuple& args, const Py::Dict& kwargs)
{
    if (args.length() > 0) {
        throw Py::RuntimeError("Excessive positinal argument");
    }
    if (kwargs.length() > 0) {
        auto name = ConvertStringObjectToString(kwargs.keys()[0]);
        throw Py::RuntimeError("Excessive named argument '" + name + "'");
    }

}

////////////////////////////////////////////////////////////////////////////////

TGilGuard::TGilGuard()
    : State_(PyGILState_Ensure())
{ }

TGilGuard::~TGilGuard()
{
    PyGILState_Release(State_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
