#include "helpers.h"

#include <yt/yt/core/ytree/attributes.h>
// TODO(gudqeit): remove after refactoring yt/yt/core/ytree/helpers-inl.h
#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NClickHouseServer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class Callback>
void ProcessInstances(const THashMap<TString, IAttributeDictionaryPtr>& instances, Callback callback)
{
    for (const auto& [key, attributes] : instances) {
        if (!attributes || !attributes->Contains("clique_incarnation")) {
            continue;
        }
        auto cliqueIncarnation = attributes->Get<i64>("clique_incarnation");
        callback(cliqueIncarnation, key, attributes);
    }
}

i64 FindMaxIncarnation(const THashMap<TString, IAttributeDictionaryPtr>& instances)
{
    i64 maxIncarnation = -1;
    auto callback = [&maxIncarnation] (i64 incarnation, const TString& /*key*/, const IAttributeDictionaryPtr& /*attributes*/) {
        maxIncarnation = std::max(maxIncarnation, incarnation);
    };
    ProcessInstances(instances, callback);
    return maxIncarnation;
}

THashMap<TString, IAttributeDictionaryPtr> FilterInstancesByIncarnation(const THashMap<TString, IAttributeDictionaryPtr>& instances)
{
    auto maxIncarnation = FindMaxIncarnation(instances);
    THashMap<TString, IAttributeDictionaryPtr> result;
    auto callback = [&] (i64 incarnation, const TString& key, const IAttributeDictionaryPtr& attributes) {
        if (incarnation == maxIncarnation) {
            result.emplace(key, attributes);
        }
    };
    ProcessInstances(instances, callback);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
