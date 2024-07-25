#include "helpers.h"
#include "shutdown.h"

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/attributes.h>

namespace Py {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

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

TStringBuf ConvertToStringBuf(PyObject* pyString)
{
    char* stringData;
    Py_ssize_t length;
    if (PyBytes_AsStringAndSize(pyString, &stringData, &length) == -1) {
        throw Exception();
    }
    return TStringBuf(stringData, length);
}

TStringBuf ConvertToStringBuf(const Bytes& pyString)
{
    return ConvertToStringBuf(pyString.ptr());
}

TString ConvertStringObjectToString(const Object& obj)
{
    Object pyString = obj;
    if (!PyBytes_Check(pyString.ptr())) {
        if (PyUnicode_Check(pyString.ptr())) {
            pyString = Object(PyUnicode_AsUTF8String(pyString.ptr()), true);
        } else {
            throw RuntimeError("Object '" + Repr(pyString) + "' is not bytes or unicode string");
        }
    }
    char* stringData;
    Py_ssize_t length;
    PyBytes_AsStringAndSize(pyString.ptr(), &stringData, &length);
    return TString(stringData, length);
}

Bytes ConvertToPythonString(TStringBuf string)
{
    return Bytes(string.begin(), string.length());
}

i64 ConvertToLongLong(const Object& obj)
{
    return static_cast<i64>(LongLong(obj));
}

std::optional<Object> FindAttr(const Object& obj, const std::string& fieldName)
{
    if (!obj.hasAttr(fieldName)) {
        return std::nullopt;
    }
    return obj.getAttr(fieldName);
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

TString Str(const Object& obj)
{
    auto stdString = obj.str().as_std_string("utf-8", "replace");
    return TString(stdString);
}

Object CreateIterator(const Object& obj)
{
    PyObject* iter = PyObject_GetIter(obj.ptr());
    if (!iter) {
        throw Exception();
    }
    return Object(iter, true);
}

TError BuildErrorFromPythonException(bool clear)
{
    PyObject* errorTypeRaw;
    PyObject* errorValueRaw;
    PyObject* errorBacktraceRaw;
    PyErr_Fetch(&errorTypeRaw, &errorValueRaw, &errorBacktraceRaw);

    Object errorType;
    if (errorTypeRaw) {
        errorType = errorTypeRaw;
    }
    Object errorValue;
    if (errorValueRaw) {
        errorValue = errorValueRaw;
    }
    Object errorBacktrace;
    if (errorBacktraceRaw) {
        errorBacktrace = errorBacktraceRaw;
    }

    auto restoreGuard = Finally([&] {
        if (!clear) {
            PyErr_Restore(errorType.ptr(), errorValue.ptr(), errorBacktrace.ptr());
        }
    });

    if (errorType.isNone()) {
        return TError();
    }

    TString message = errorValue.isNone()
        ? "No message"
        : Str(errorValue);
    auto error = TError(message)
        << TErrorAttribute("exception_type", Str(errorType));

    if (!errorBacktrace.isNone()) {
        error.MutableAttributes()->Set("backtrace", Str(errorBacktrace));
    }

    return error;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace Py

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Object ExtractArgument(Py::Tuple& args, Py::Dict& kwargs, const std::string& name)
{
    Py::Object result;
    if (kwargs.hasKey(name)) {
        result = kwargs.getItem(name);
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
    , ThreadId_(GetCurrentThreadId())
{ }

TGilGuard::~TGilGuard()
{
    YT_VERIFY(ThreadId_ == GetCurrentThreadId());
    PyGILState_Release(State_);
}

////////////////////////////////////////////////////////////////////////////////

TReleaseAcquireGilGuard::TReleaseAcquireGilGuard()
    : State_(PyEval_SaveThread())
    , ThreadId_(GetCurrentThreadId())
{
    EnterReleaseAcquireGuard();
}

TReleaseAcquireGilGuard::~TReleaseAcquireGilGuard()
{
    // NB: this check is outdated, but it worth to remember about it.
    // Now the problem is solved by waiting that all threads leave this guard in BeforeShutdown.
    //
    // See YT-13246 for details.
    // Suppose the following situation:
    // 1. Some thread has acquired TReleaseAcquireGilGuard and then tries to acquire GIL,
    // 2. At the same time finalization has started;
    // In this case c-python runtime tries to exit the thread on attempt to acquire GIL;
    // this causes unwind of the stack, that executes this destructor,
    // but PyEval_RestoreThread checks existence of GIL that can be already destroyed.
    // if (!Py_IsInitialized()) {
    //     return;
    // }
    YT_VERIFY(ThreadId_ == GetCurrentThreadId());
    PyEval_RestoreThread(State_);

    LeaveReleaseAcquireGuard();
}

////////////////////////////////////////////////////////////////////////////////

TPythonClassObject::TPythonClassObject()
{ }

TPythonClassObject::TPythonClassObject(PyTypeObject* typeObject)
    : ClassObject_(Py::Callable(reinterpret_cast<PyObject*>(typeObject)))
{ }

Py::Callable TPythonClassObject::Get()
{
    return ClassObject_;
}

////////////////////////////////////////////////////////////////////////////////

PyObject* FindModuleAttribute(const TString& moduleName, const TString& attributeName)
{
#if PY_MAJOR_VERSION < 3
    auto module = PyObjectPtr(PyImport_ImportModuleNoBlock(moduleName.c_str()));
#else
    auto module = PyObjectPtr(PyImport_ImportModule(moduleName.c_str()));
#endif
    if (!module) {
        throw Py::ImportError(Format("No module named %Qv", moduleName));
    }
    if (!PyObject_HasAttrString(module.get(), attributeName.c_str())) {
        return nullptr;
    }
    return PyObject_GetAttrString(module.get(), attributeName.c_str());
}

PyObject* GetModuleAttribute(const TString& moduleName, const TString& attributeName)
{
    auto attribute = FindModuleAttribute(moduleName, attributeName);
    if (!attribute) {
        throw Py::ImportError(Format("Cannot import name %Qv from module %Qv",
            attributeName,
            moduleName));
    }
    return attribute;
}

PyObject* FindYsonTypeClass(const std::string& name)
{
    return FindModuleAttribute("yt.yson.yson_types", TString(name));
}

PyObject* GetYsonTypeClass(const std::string& name)
{
    auto klass = FindYsonTypeClass(name);
    if (!klass) {
        throw Py::RuntimeError("Class " + name + " not found in module yt.yson.yson_types");
    }
    return klass;
}

////////////////////////////////////////////////////////////////////////////////

bool WaitForSettingFuture(TFuture<void> future)
{
    while (true) {
        if (future.Wait(TDuration::MilliSeconds(100))) {
            return true;
        }

        {
            TGilGuard guard;
            auto signals = PyErr_CheckSignals();
            if (signals == -1) {
                return false;
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
