#include "helpers.h"

#include "requests.h"

#include <mapreduce/yt/interface/logging/yt_log.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TString GetFullUrl(const TString& hostName, const TAuth& auth, THttpHeader& header)
{
    Y_UNUSED(auth);
    return Format("http://%v%v", hostName, header.GetUrl());
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
