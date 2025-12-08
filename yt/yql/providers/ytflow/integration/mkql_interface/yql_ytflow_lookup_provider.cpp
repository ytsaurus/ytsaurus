#include "yql_ytflow_lookup_provider.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/hash.h>


namespace NYql {

namespace {

class TYtflowLookupProviderRegistry
    : public IYtflowLookupProviderRegistry
{
public:
    TYtflowLookupProviderRegistry() = default;

    void Register(const TString& providerName, TCreationCallback callback) override
    {
        auto [_, emplaced] = CreationCallbacks.emplace(providerName, std::move(callback));

        YQL_ENSURE(emplaced, "Duplicate lookup provider registration: " << providerName);
    }

    THolder<IYtflowLookupProvider> Create(
        const TString& providerName,
        TCreationContext& ctx
    ) const override {
        auto iterator = CreationCallbacks.find(providerName);
        YQL_ENSURE(
            iterator != CreationCallbacks.end(),
            "Unknown lookup provider: " << providerName);

        return iterator->second(ctx);
    }

private:
    THashMap<TString, TCreationCallback> CreationCallbacks;
};

} // anonymous namespace

THolder<IYtflowLookupProviderRegistry> CreateYtflowLookupProviderRegistry()
{
    return MakeHolder<TYtflowLookupProviderRegistry>();
}

} // namespace NYql
