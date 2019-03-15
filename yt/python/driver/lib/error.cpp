#include "error.h"

#include <yt/core/ytree/convert.h>

namespace NYT::NPython {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

Py::Exception CreateYtError(const TString& message, const TError& error)
{
    return CreateYtError(message, ConvertTo<Py::Object>(std::vector<TError>({error})));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

