#include "error.h"

#include <yt/python/common/helpers.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const TString& message, const TError& error)
{
    return CreateYsonError(message, NYTree::ConvertTo<Py::Object>(std::vector<TError>({error})));
}

Py::Exception CreateYsonError(const TString& message, TContext* context)
{
    thread_local PyObject* ysonErrorClass = nullptr;
    if (!ysonErrorClass) {
        auto ysonModule = Py::Module(PyImport_ImportModule("yt.yson.common"), /* owned */ true);
        ysonErrorClass = PyObject_GetAttrString(ysonModule.ptr(), "YsonError");
    }

    Py::Dict attributes;
    if (context) {
        if (context->RowIndex) {
            attributes.setItem("row_index", Py::Long(*context->RowIndex));
        }

        bool endedWithDelimiter = false;
        TStringBuilder builder;
        for (const auto& pathPart : context->PathParts) {
            if (pathPart.InAttributes) {
                YCHECK(!endedWithDelimiter);
                builder.AppendString("/@");
                endedWithDelimiter = true;
            } else {
                if (!endedWithDelimiter) {
                    builder.AppendChar('/');
                }
                if (!pathPart.Key.empty()) {
                    builder.AppendString(pathPart.Key);
                }
                if (pathPart.Index != -1) {
                    builder.AppendFormat("%v", pathPart.Index);
                }
                endedWithDelimiter = false;
            }
        }

        TString contextRowKeyPath = builder.Flush();
        if (!contextRowKeyPath.empty()) {
            attributes.setItem("row_key_path", Py::ConvertToPythonString(contextRowKeyPath));
        }
    }

    Py::Dict options;
    options.setItem("message", Py::ConvertToPythonString(TString(message)));
    options.setItem("code", Py::Long(1));
    options.setItem("attributes", attributes);

    auto ysonError = Py::Callable(ysonErrorClass).apply(Py::Tuple(), options);
    return Py::Exception(*ysonError.type(), ysonError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
