#include "error.h"

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYsonError(const TString& message, const TError& error)
{
    return CreateYsonError(message, NYTree::ConvertTo<Py::Object>(std::vector<TError>({error})));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
