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

    void Register(const TString& providerName, TFactoryCreationCallback callback) override
    {
        auto [_, emplaced] = FactoryCreationCallbacks.emplace(providerName, std::move(callback));

        YQL_ENSURE(emplaced, "Duplicate lookup provider registration: " << providerName);
    }

    THolder<IYtflowLookupProviderFactory> CreateFactory(
        const TString& providerName,
        const TFactoryCreationContext& ctx
    ) const override {
        auto iterator = FactoryCreationCallbacks.find(providerName);
        YQL_ENSURE(
            iterator != FactoryCreationCallbacks.end(),
            "Unknown lookup provider: " << providerName);

        return iterator->second(ctx);
    }

private:
    THashMap<TString, TFactoryCreationCallback> FactoryCreationCallbacks;
};

} // anonymous namespace

THolder<IYtflowLookupProviderRegistry> CreateYtflowLookupProviderRegistry()
{
    return MakeHolder<TYtflowLookupProviderRegistry>();
}

} // namespace NYql
