#include "helpers.h"

namespace Py {

///////////////////////////////////////////////////////////////////////////////

bool IsInstance(const Object& obj, const Object& cls)
{
    return PyObject_IsInstance(*obj, *cls) == 1;
}

bool IsStringLike(const Object& obj)
{
    return PyString_Check(obj.ptr());
}

String ConvertToString(const Object& obj)
{
    return String(PyObject_Str(*obj), true);
}

TStringBuf ConvertToStringBuf(const String& pyString)
{
    char* stringData;
    Py_ssize_t length;
    PyString_AsStringAndSize(pyString.ptr(), &stringData, &length);
    return TStringBuf(stringData, length);
}

Stroka ConvertToStroka(const String& pyString)
{
    char* stringData;
    Py_ssize_t length;
    PyString_AsStringAndSize(pyString.ptr(), &stringData, &length);
    return Stroka(stringData, length);
}

String ConvertToPythonString(const Stroka& string)
{
    return Py::String(string.c_str(), string.length());
}

Object GetAttr(const Object& obj, const std::string& fieldName)
{
    if (!obj.hasAttr(fieldName)) {
        throw RuntimeError("There is no field " + fieldName);
    }
    return obj.getAttr(fieldName);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace Py

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

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
        auto name = ConvertToStroka(ConvertToString(kwargs.keys()[0]));
        throw Py::RuntimeError("Excessive named argument '" + name + "'");
    }

}

///////////////////////////////////////////////////////////////////////////////

TGilGuard::TGilGuard()
    : State_(PyGILState_Ensure())
{ }

TGilGuard::~TGilGuard()
{
    PyGILState_Release(State_);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
