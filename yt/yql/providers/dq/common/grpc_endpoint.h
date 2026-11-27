#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NYql {

    TString MakeDqGrpcEndpoint(TStringBuf host, TStringBuf port);

} // namespace NYql
