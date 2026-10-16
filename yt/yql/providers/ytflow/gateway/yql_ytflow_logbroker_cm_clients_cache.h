#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <util/digest/multi.h>
#include <util/generic/string.h>

namespace NPq::NConfigurationManager {

struct IClient;

} // namespace NPq::NConfigurationManager

namespace NYql::NYtflow {

struct TCmConnectionConfig {
    TString CmEndpoint;
    TString Token;
    bool AddBearerToToken = false;
    bool UseSsl = false;

    bool operator==(const TCmConnectionConfig& other) const = default;
};

DECLARE_REFCOUNTED_CLASS(ILogbrokerCmClientsCache);

using TLogbrokerCmClientPtr = TIntrusivePtr<NPq::NConfigurationManager::IClient>;

class ILogbrokerCmClientsCache
    : public NYT::TRefCounted
{
public:
    virtual TLogbrokerCmClientPtr GetCmClient(
        const TString& cluster,
        const TCmConnectionConfig& config) = 0;
};

ILogbrokerCmClientsCachePtr CreateLogbrokerCmClientsCache();

} // namespace NYql::NYtflow

template<>
struct THash<NYql::NYtflow::TCmConnectionConfig>
{
    size_t operator()(const NYql::NYtflow::TCmConnectionConfig& config) const
    {
        return MultiHash(config.CmEndpoint, config.Token, config.AddBearerToToken, config.UseSsl);
    }
};
