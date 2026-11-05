#pragma once

#include <util/generic/fwd.h>


namespace NYql {

void ConductorGroupToHosts(TStringBuf group, TVector<TString>* hosts);

} // namespace NYql
