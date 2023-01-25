#include "error.h"

#include <yt/yt/python/yson/serialize.h>

#include <yt/yt/python/common/helpers.h>

#include <yt/yt/core/ytree/convert.h>


namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateSkiffError(const TString& message, const TError& error, const TSkiffRowContext* rowContext)
{
    auto innerErrors = NYTree::ConvertTo<Py::Object>(std::vector<TError>({error}));

    static auto* skiffErrorClass = GetModuleAttribute("yt.wrapper.schema", "SkiffError");

    Py::Dict attributes;
    if (rowContext) {
        if (rowContext->RowIndex != -1) {
            attributes.setItem("row_index", Py::Long(rowContext->RowIndex));
        }
        attributes.setItem("table_index", Py::Long(rowContext->TableIndex));
    }

    Py::Dict options;
    options.setItem("message", Py::ConvertToPythonString(message));
    options.setItem("code", Py::Long(1));
    options.setItem("attributes", attributes);
    options.setItem("inner_errors", innerErrors);

    auto skiffError = Py::Callable(skiffErrorClass).apply(Py::Tuple(), options);
    return Py::Exception(*skiffError.type(), skiffError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
