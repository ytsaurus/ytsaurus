#include "helpers.h"

#include <yt/yt/python/common/helpers.h>

namespace NYT::NPython {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

EConnectionType ParseConnectionType(const Py::Object& obj)
{
    try {
        return ParseEnum<EConnectionType>(ConvertStringObjectToString(obj));
    } catch (const std::exception& ex) {
        throw Py::RuntimeError(TString("Error parsing \"connection_type\"\n") + ex.what());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
