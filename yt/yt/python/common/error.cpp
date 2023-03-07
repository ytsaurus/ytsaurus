#include "error.h"
#include "helpers.h"

#include <yt/core/ytree/convert.h>

#include <Extensions.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateError(const Py::Callable& errorClass, const TString& message, const Py::Object& innerErrors)
{
    Py::Dict options;
    options.setItem("message", Py::ConvertToPythonString(message));
    options.setItem("code", Py::Int(1));
    if (innerErrors.isNone()) {
        options.setItem("inner_errors", Py::List());
    } else {
        options.setItem("inner_errors", innerErrors);
    }
    auto errorObject = errorClass.apply(Py::Tuple(), options);
    return Py::Exception(*errorObject.type(), errorObject);
}

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYtError(const TString& message, const Py::Object& innerErrors)
{
    auto ytModule = Py::Module(PyImport_ImportModule("yt.common"), true);
    auto ytErrorClass = Py::Callable(GetAttr(ytModule, "YtError"));

    return CreateError(ytErrorClass, message, innerErrors);
}

Py::Exception CreateYtError(const TString& message)
{
    return CreateYtError(message, Py::None());
}

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const TString& message, const Py::Object& innerErrors)
{
    auto ysonModule = Py::Module(PyImport_ImportModule("yt.yson.common"), true);
    auto ysonErrorClass = Py::Callable(GetAttr(ysonModule, "YsonError"));

    return CreateError(ysonErrorClass, message, innerErrors);
}

Py::Exception CreateYsonError(const TString& message)
{
    return CreateYsonError(message, Py::None());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
