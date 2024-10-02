#include "object_service_proxy.h"

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

TString ConvertCppToGrpcNamespace(TStringBuf cppNamespace)
{
    TString result;
    result.reserve(cppNamespace.size());
    for (size_t i = 0; i < cppNamespace.size(); ) {
        if (i + 1 < cppNamespace.size() && cppNamespace[i] == ':' && cppNamespace[i + 1] == ':') {
            result.push_back('.');
            i += 2;
        } else {
            result.push_back(cppNamespace[i]);
            i += 1;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
