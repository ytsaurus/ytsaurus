#include "grpc_endpoint.h"

#include <util/string/builder.h>

namespace NYql {

    TString MakeDqGrpcEndpoint(TStringBuf host, TStringBuf port)
    {
        TStringBuilder endpoint;
        if (host.Contains(':') && !host.StartsWith("[")) {
            endpoint << '[' << host << ']';
        } else {
            endpoint << host;
        }
        return endpoint << ':' << port;
    }

} // namespace NYql
