#include "cache.h"
#include "options.h"
#include "rpc.h"

#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NClient::NHedging::NRpc {

namespace {

class TClientsCache: public IClientsCache {
public:
    TClientsCache(const TConfig& config, const NYT::NApi::TClientOptions& options)
        : CommonConfig_(config)
        , Options_(options)
    {}

    NYT::NApi::IClientPtr GetClient(TStringBuf clusterUrl) override {
        {
            auto guard = ReaderGuard(Lock_);
            auto clientIt = Clients_.find(clusterUrl);
            if (clientIt != Clients_.end()) {
                return clientIt->second;
            }
        }

        TConfig config = CommonConfig_;
        SetClusterUrl(config, clusterUrl);
        auto client = CreateClient(config, Options_);

        auto guard = WriterGuard(Lock_);
        return Clients_.try_emplace(clusterUrl, client).first->second;
    }

private:
    TConfig CommonConfig_;
    NYT::NApi::TClientOptions Options_;
    NYT::NThreading::TReaderWriterSpinLock Lock_;
    THashMap<TString, NYT::NApi::IClientPtr> Clients_;
};

} // namespace

IClientsCachePtr CreateClientsCache(const TConfig& config, const NYT::NApi::TClientOptions& options) {
    return NYT::New<TClientsCache>(config, options);
}

IClientsCachePtr CreateClientsCache(const TConfig& config) {
    return CreateClientsCache(config, GetClientOpsFromEnvStatic());
}

IClientsCachePtr CreateClientsCache(const NYT::NApi::TClientOptions& options) {
    return CreateClientsCache({}, options);
}

IClientsCachePtr CreateClientsCache() {
    return CreateClientsCache(GetClientOpsFromEnvStatic());
}

} // namespace NYT::NClient::NHedging::NRpc
