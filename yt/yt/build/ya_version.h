#pragma once

#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString CreateYTVersion(int major, int minor, int patch, TStringBuf branch);
TString GetYaHostName();
TString GetYaBuildDate();
TString GetRpcUserAgent();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

