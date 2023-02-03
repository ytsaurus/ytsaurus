#pragma once

#include "fwd.h"

#include "http.h"

#include <util/generic/fwd.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TString CreateHostNameWithPort(const TString& name, const TAuth& auth);

TString GetFullUrl(const TString& hostName, const TAuth& auth, THttpHeader& header);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
