#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/stroka.h>

#include <contrib/libs/pycxx/Objects.hxx>

#include <util/generic/noncopyable.h>

namespace Py {

///////////////////////////////////////////////////////////////////////////////
// Extend PyCxx with some useful functions.

bool IsInstance(const Object& obj, const Object& cls);
bool IsStringLike(const Object& obj);
String ConvertToString(const Object& obj);
TStringBuf ConvertToStringBuf(const String& pyString);
Stroka ConvertToStroka(const String& pyString);
String ConvertToPythonString(const Stroka& string);
Object GetAttr(const Object& obj, const std::string& fieldName);

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
