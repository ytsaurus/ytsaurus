#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TSchemaListCacheKey {
        TString Zone;
        TString Cluster;
        TString Folder;

        friend bool operator==(
            const TSchemaListCacheKey& lhs,
            const TSchemaListCacheKey& rhs) = default;
    };

} // namespace NSQLComplete

template <>
struct THash<NSQLComplete::TSchemaListCacheKey> {
    inline size_t operator()(const NSQLComplete::TSchemaListCacheKey& key) const {
        return THash<std::tuple<TString, TString, TString>>()(
            std::tie(key.Zone, key.Cluster, key.Folder));
    }
};
