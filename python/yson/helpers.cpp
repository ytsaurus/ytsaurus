#include "helpers.h"
#include "error.h"

#include <yt/python/common/helpers.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

std::optional<TString> ParseEncodingArgument(Py::Tuple& args, Py::Dict& kwargs)
{
    std::optional<TString> encoding;
    if (HasArgument(args, kwargs, "encoding")) {
        auto arg = ExtractArgument(args, kwargs, "encoding");
        if (!arg.isNone()) {
#if PY_MAJOR_VERSION < 3
            throw CreateYsonError("Encoding parameter is not supported for Python 2");
#else
            encoding = ConvertStringObjectToString(arg);
#endif
        }
#if PY_MAJOR_VERSION >= 3
    } else {
        encoding = "utf-8";
#endif
    }

    return encoding;
}

Py::Bytes EncodeStringObject(const Py::Object& obj, const std::optional<TString>& encoding, TContext* context)
{
    if (PyUnicode_Check(obj.ptr())) {
        if (!encoding) {
#if PY_MAJOR_VERSION >= 3
            PyObject* bytesString = nullptr;
#else
            PyObject* bytesString = PyUnicode_AsEncodedString(obj.ptr(), "ascii", "strict");
#endif
            if (!bytesString) {
                throw CreateYsonError(
                    Format(
                        "Cannot encode unicode object %v to bytes "
                        "since \"encoding\" parameter is None",
                        Py::Repr(obj)
                    ),
                    context);
            } else {
                return Py::Bytes(bytesString, true);
            }
        }
        return Py::Bytes(PyUnicode_AsEncodedString(obj.ptr(), encoding->data(), "strict"), true);
    } else {
        return Py::Bytes(PyObject_Bytes(*obj), true);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

