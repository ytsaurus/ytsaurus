#include "rpc_proxy_discovery.h"


#include <mapreduce/yt/interface/client.h>


namespace NYT {
    // FIXME: this should be removed when YT RPC proxy discovery is shipped
    yvector<TString> GetRpcProxyHosts(const TString& proxy, const TString& token) {
        NYT::IClientPtr clientPtr = NYT::CreateClient(proxy, NYT::TCreateClientOptions().Token(token));

        yvector<TString> proxies;
        for (const auto& proxyUrl : clientPtr->List("//sys/rpc_proxies"))
            proxies.push_back(proxyUrl.AsString());
        return proxies;
    }
}
