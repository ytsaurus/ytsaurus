#pragma once

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/ref.h>

#include <yt/core/ytree/public.h>

#include <util/generic/noncopyable.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <contrib/libs/pycxx/Objects.hxx>

namespace Py {

////////////////////////////////////////////////////////////////////////////////
// Extend PyCxx with some useful functions.

static_assert(sizeof(PY_LONG_LONG) == sizeof(i64), "Py_LONG_LONG size does not equal i64 size");

bool IsInstance(const Object& obj, const Object& cls);
bool IsInteger(const Object& obj);
bool IsFloat(const Object& obj);
TStringBuf ConvertToStringBuf(const Bytes& pyString);
Bytes ConvertToPythonString(const TString& string);
TString ConvertStringObjectToString(const Object& obj);
Object GetAttr(const Object& obj, const std::string& fieldName);
i64 ConvertToLongLong(const Object& obj);
std::string Repr(const Object& obj);

////////////////////////////////////////////////////////////////////////////////

} // namespace Py

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object ExtractArgument(Py::Tuple& args, Py::Dict& kwargs, const std::string& name);
bool HasArgument(const Py::Tuple& args, const Py::Dict& kwargs, const std::string& name);
void ValidateArgumentsEmpty(const Py::Tuple& args, const Py::Dict& kwargs);

////////////////////////////////////////////////////////////////////////////////

class TGilGuard
    : private TNonCopyable
{
public:
    TGilGuard();
    ~TGilGuard();

private:
    PyGILState_STATE State_;
};

////////////////////////////////////////////////////////////////////////////////

class TReleaseAcquireGilGuard
    : private TNonCopyable
{
public:
    TReleaseAcquireGilGuard();
    ~TReleaseAcquireGilGuard();

private:
    PyThreadState* State_;
};

////////////////////////////////////////////////////////////////////////////////

class TPythonClassObject
{
public:
    TPythonClassObject();
    explicit TPythonClassObject(PyTypeObject* typeObject);
    Py::Callable Get();

private:
    Py::Callable ClassObject_;
};

////////////////////////////////////////////////////////////////////////////////

class TPythonStringCache
{
public:
    TPythonStringCache();
    TPythonStringCache(bool enableCache, const TNullable<TString>& encoding);
    PyObject* GetPythonString(const TStringBuf& string);

private:
    bool CacheEnabled_;
    std::unique_ptr<THashMap<TStringBuf, PyObject*>> Cache_;
    TNullable<TString> Encoding_;
};

////////////////////////////////////////////////////////////////////////////////

Py::Callable GetYsonTypeClass(const std::string& name);

////////////////////////////////////////////////////////////////////////////////

bool WaitForSettingFuture(TFuture<void> future);

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
