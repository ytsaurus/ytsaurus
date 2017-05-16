#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <contrib/libs/pycxx/Objects.hxx>

namespace Py {

///////////////////////////////////////////////////////////////////////////////
// Extend PyCxx with some useful functions.

static_assert(sizeof(PY_LONG_LONG) == sizeof(i64), "Py_LONG_LONG size does not equal i64 size");

bool IsInstance(const Object& obj, const Object& cls);
bool IsInteger(const Object& obj);
bool IsFloat(const Object& obj);
TStringBuf ConvertToStringBuf(const Bytes& pyString);
Bytes ConvertToPythonString(const TString& string);
TString ConvertStringObjectToStroka(const Object& obj);
Object GetAttr(const Object& obj, const std::string& fieldName);
i64 ConvertToLongLong(const Object& obj);
std::string Repr(const Object& obj);

///////////////////////////////////////////////////////////////////////////////

} // namespace Py

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

Py::Object ExtractArgument(Py::Tuple& args, Py::Dict& kwargs, const std::string& name);
bool HasArgument(const Py::Tuple& args, const Py::Dict& kwargs, const std::string& name);
void ValidateArgumentsEmpty(const Py::Tuple& args, const Py::Dict& kwargs);

///////////////////////////////////////////////////////////////////////////////

class TGilGuard
    : private TNonCopyable
{
public:
    TGilGuard();
    ~TGilGuard();

private:
    PyGILState_STATE State_;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
