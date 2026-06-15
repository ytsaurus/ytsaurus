#pragma once

#include <util/generic/fwd.h>


namespace NYql {

void DeployServiceToHosts(TStringBuf service, ui32 expectedHosts, TVector<TString>* hosts);

} // namespace NYql
