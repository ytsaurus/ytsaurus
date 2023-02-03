#include "helpers.h"

#include "requests.h"

#include <mapreduce/yt/interface/logging/yt_log.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

TString CreateHostNameWithPort(const TString& hostName, const TAuth& auth)
{
    static constexpr int HttpProxyPort = 80;
    static constexpr int HttpsProxyPort = 443;

    static constexpr int TvmOnlyHttpProxyPort = 9026;
    static constexpr int TvmOnlyHttpsProxyPort = 9443;

    if (hostName.find(':') == TString::npos) {
        int port;
        if (auth.TvmOnly) {
            port = auth.UseTLS
                ? TvmOnlyHttpsProxyPort
                : TvmOnlyHttpProxyPort;
        } else {
            port = auth.UseTLS
                ? HttpsProxyPort
                : HttpProxyPort;
        }
        return Format("%v:%v", hostName, port);
    }
    return hostName;
}

TString GetFullUrl(const TString& hostName, const TAuth& auth, THttpHeader& header)
{
    Y_UNUSED(auth);
    return Format("http://%v%v", hostName, header.GetUrl());
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
