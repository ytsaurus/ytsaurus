#include "mount_config_attributes.h"

#include "private.h"
#include "table_node.h"

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/tablet_server/mount_config_storage.h>

#include <yt/yt/server/master/object_server/attribute_set.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NTabletNode;
using namespace NYson;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Enumerates the attributes which were present in mount config by the moment
// this code was written. Used for compatibility only.
bool IsOldStyleMountConfigAttribute(TStringBuf key)
{
    // NB: Do not change this list even if fields are added or removed
    // to the mount config.
    static constexpr std::array Keys = {
        "min_data_ttl",
        "max_data_ttl",
        "min_data_versions",
        "max_data_versions",
        "ignore_major_timestamp",
        "max_dynamic_store_row_count",
        "max_dynamic_store_value_count",
        "max_dynamic_store_timestamp_count",
        "max_dynamic_store_pool_size",
        "max_dynamic_store_row_data_weight",
        "dynamic_store_overflow_threshold",
        "max_partition_data_size",
        "desired_partition_data_size",
        "min_partition_data_size",
        "max_partition_count",
        "min_partitioning_data_size",
        "min_partitioning_store_count",
        "max_partitioning_data_size",
        "max_partitioning_store_count",
        "min_compaction_store_count",
        "max_compaction_store_count",
        "compaction_data_size_base",
        "compaction_data_size_ratio",
        "flush_throttler",
        "compaction_throttler",
        "partitioning_throttler",
        "throttlers",
        "samples_per_partition",
        "backing_store_retention_time",
        "max_read_fan_in",
        "max_overlapping_store_count",
        "critical_overlapping_store_count",
        "overlapping_store_immediate_split_threshold",
        "max_stores_per_tablet",
        "max_eden_stores_per_tablet",
        "forced_chunk_view_compaction_revision",
        "dynamic_store_auto_flush_period",
        "dynamic_store_flush_period_splay",
        "auto_compaction_period",
        "auto_compaction_period_splay_ratio",
        "periodic_compaction_mode",
        "enable_lookup_hash_table",
        "lookup_cache_rows_per_tablet",
        "lookup_cache_rows_ratio",
        "enable_lookup_cache_by_default",
        "row_count_to_keep",
        "replication_tick_period",
        "min_replication_log_ttl",
        "max_timestamps_per_replication_commit",
        "max_rows_per_replication_commit",
        "max_data_weight_per_replication_commit",
        "replication_throttler",
        "relative_replication_throttler",
        "enable_replication_logging",
        "enable_profiling",
        "enable_structured_logger",
        "enable_compaction_and_partitioning",
        "enable_store_rotation",
        "merge_rows_on_flush",
        "merge_deletions_on_flush",
        "enable_lsm_verbose_logging",
        "max_unversioned_block_size",
        "preserve_tablet_index",
        "enable_partition_split_while_eden_partitioning",
        "enable_discarding_expired_partitions",
        "enable_data_node_lookup",
        "enable_peer_probing_in_data_node_lookup",
        "enable_rejects_in_data_node_lookup_if_throttling",
        "lookup_rpc_multiplexing_parallelism",
        "enable_new_scan_reader_for_lookup",
        "enable_new_scan_reader_for_select",
        "enable_hunk_columnar_profiling",
        "max_hunk_compaction_garbage_ratio",
        "max_hunk_compaction_size",
        "hunk_compaction_size_base",
        "hunk_compaction_size_ratio",
        "min_hunk_compaction_chunk_count",
        "max_hunk_compaction_chunk_count",
        "precache_chunk_replicas_on_mount",
        "register_chunk_replicas_on_stores_update",
        "enable_replication_progress_advance_to_barrier",
    };

    static_assert(Keys.size() == 81, "This list should not be modified");

    static const THashSet<TStringBuf> KeySet(Keys.begin(), Keys.end());

    return KeySet.contains(key);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TMountConfigAttributeDictionary::TMountConfigAttributeDictionary(
    TBootstrap* bootstrap,
    TTableNode* owner,
    TTransaction* transaction,
    NYTree::IAttributeDictionary* baseAttributes,
    bool includeOldAttributesInList)
    : Bootstrap_(bootstrap)
    , Owner_(owner)
    , Transaction_(transaction)
    , BaseAttributes_(baseAttributes)
    , IncludeOldAttributesInList_(includeOldAttributesInList)
{ }

auto TMountConfigAttributeDictionary::ListKeys() const -> std::vector<TKey>
{
    if (!IncludeOldAttributesInList_) {
        return BaseAttributes_->ListKeys();
    }

    auto storage = Owner_->FindMountConfigStorage();
    if (!storage || storage->IsEmpty()) {
        return BaseAttributes_->ListKeys();
    }

    auto result = BaseAttributes_->ListKeys();
    for (const auto& [key, value] : storage->Attributes()) {
        if (NDetail::IsOldStyleMountConfigAttribute(key)) {
            result.push_back(key);
        }
    }
    return result;
}

auto TMountConfigAttributeDictionary::ListPairs() const -> std::vector<TKeyValuePair>
{
    if (!IncludeOldAttributesInList_) {
        return BaseAttributes_->ListPairs();
    }

    auto storage = Owner_->FindMountConfigStorage();
    if (!storage || storage->IsEmpty()) {
        return BaseAttributes_->ListPairs();
    }

    auto result = BaseAttributes_->ListPairs();
    for (const auto& [key, value] : storage->Attributes()) {
        if (NDetail::IsOldStyleMountConfigAttribute(key)) {
            result.emplace_back(key, value);
        }
    }
    return result;
}

auto TMountConfigAttributeDictionary::FindYson(TKeyView key) const -> TValue
{
    if (NDetail::IsOldStyleMountConfigAttribute(key)) {
        if (auto storage = Owner_->FindMountConfigStorage()) {
            return storage->Find(key);
        }
        return {};
    }
    return BaseAttributes_->FindYson(key);
}

void TMountConfigAttributeDictionary::SetYson(TKeyView key, const NYson::TYsonString& value)
{
    if (NDetail::IsOldStyleMountConfigAttribute(key)) {
        auto* lockedTable = LockMountConfigAttribute();
        Owner_->GetTrunkNode()->OnRemountNeeded();
        return lockedTable->GetMutableMountConfigStorage()->Set(key, value);
    }
    return BaseAttributes_->SetYson(key, value);
}

bool TMountConfigAttributeDictionary::Remove(TKeyView key)
{
    if (NDetail::IsOldStyleMountConfigAttribute(key)) {
        auto* lockedTable = LockMountConfigAttribute();
        Owner_->GetTrunkNode()->OnRemountNeeded();
        return lockedTable->GetMutableMountConfigStorage()->TryRemove(key);
    }
    return BaseAttributes_->Remove(key);
}

TTableNode* TMountConfigAttributeDictionary::LockMountConfigAttribute()
{
    return Bootstrap_->GetCypressManager()->LockNode(
        Owner_->GetTrunkNode(),
        Transaction_,
        TLockRequest::MakeSharedAttribute(EInternedAttributeKey::MountConfig.Unintern()))->As<TTableNode>();
}

////////////////////////////////////////////////////////////////////////////////

void InternalizeMountConfigAttributes(IAttributeDictionary* attributes)
{
    std::vector<std::pair<std::string, TYsonString>> oldStyleAttributes;
    for (const auto& key : attributes->ListKeys()) {
        if (NDetail::IsOldStyleMountConfigAttribute(key)) {
            oldStyleAttributes.emplace_back(key, attributes->GetYsonAndRemove(key));
        }
    }

    if (oldStyleAttributes.empty()) {
        return;
    }

    auto mountConfigNode = attributes->FindAndRemove<IMapNodePtr>(EInternedAttributeKey::MountConfig.Unintern());
    if (!mountConfigNode) {
        mountConfigNode = GetEphemeralNodeFactory()->CreateMap();
    }

    for (auto&& [key, value] : oldStyleAttributes) {
        mountConfigNode->AddChild(key, ConvertToNode(std::move(value)));
    }

    attributes->Set(EInternedAttributeKey::MountConfig.Unintern(), mountConfigNode);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<std::pair<std::string, TYsonString>> ExtractOldStyleMountConfigAttributes(
    TAttributeSet* attributes)
{
    std::vector<std::pair<std::string, TYsonString>> result;

    for (const auto& [key, value] : attributes->Attributes()) {
        if (NDetail::IsOldStyleMountConfigAttribute(key)) {
            result.emplace_back(key, value);
        }
    }

    for (const auto& [key, value] : result) {
        attributes->TryRemove(key);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
