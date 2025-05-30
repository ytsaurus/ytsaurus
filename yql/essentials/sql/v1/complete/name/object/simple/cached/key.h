#pragma once

#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>

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

    struct TSchemaListSizeProvider {
        size_t operator()(const TSchemaListCacheKey& key) const {
            return sizeof(key) + key.Zone.size() + key.Cluster.size() + key.Folder.size();
        }

        size_t operator()(const TVector<TFolderEntry>& entries) const {
            return sizeof(entries) +
                   Accumulate(
                       entries, static_cast<size_t>(0),
                       [](size_t acc, const TFolderEntry& entry) {
                           return acc + sizeof(entry) + entry.Type.size() + entry.Name.size();
                       });
        }
    };

} // namespace NSQLComplete

template <>
struct THash<NSQLComplete::TSchemaListCacheKey> {
    inline size_t operator()(const NSQLComplete::TSchemaListCacheKey& key) const {
        return THash<std::tuple<TString, TString, TString>>()(
            std::tie(key.Zone, key.Cluster, key.Folder));
    }
};
