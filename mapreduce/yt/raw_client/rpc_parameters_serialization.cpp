#include "rpc_parameters_serialization.h"

#include <mapreduce/yt/common/helpers.h>

#include <mapreduce/yt/interface/client_method_options.h>
#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/node/node.h>
#include <mapreduce/yt/node/node_io.h>
#include <mapreduce/yt/node/node_builder.h>

#include <util/generic/guid.h>
#include <util/string/cast.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////

static void SetTransactionIdParam(TNode* node, const TTransactionId& transactionId)
{
    if (transactionId != TTransactionId()) {
        (*node)["transaction_id"] = GetGuidAsString(transactionId);
    }
}

static void SetPathParam(TNode* node, const TYPath& path)
{
    (*node)["path"] = AddPathPrefix(path);
}

static TNode SerializeAttributeFilter(const TAttributeFilter& attributeFilter)
{
    TNode result;
    for (const auto& attribute : attributeFilter.Attributes_) {
        result.Add(attribute);
    }
    return result;
}

////////////////////////////////////////////////////////////////////

TNode SerializeParamsForCreate(
    const TTransactionId& transactionId,
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
    result["recursive"] = options.Recursive_;
    result["type"] = ::ToString(type);
    result["ignore_existing"] = options.IgnoreExisting_;
    result["force"] = options.Force_;
    if (options.Attributes_) {
        result["attributes"] = *options.Attributes_;
    }
    return result;
}

TNode SerializeParamsForRemove(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
    result["recursive"] = options.Recursive_;
    result["force"] = options.Force_;
    return result;
}

TNode SerializeParamsForExists(
    const TTransactionId& transactionId,
    const TYPath& path)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
    return result;
}

TNode SerializeParamsForGet(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
    if (options.AttributeFilter_) {
        result["attributes"] = SerializeAttributeFilter(*options.AttributeFilter_);
    }
    if (options.MaxSize_) {
        result["max_size"] = *options.MaxSize_;
    }
    return result;
}

TNode SerializeParamsForSet(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TSetOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
    result["recursive"] = options.Recursive_;
    return result;
}

TNode SerializeParamsForList(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
    if (options.MaxSize_) {
        result["max_size"] = *options.MaxSize_;
    }
    if (options.AttributeFilter_) {
        result["attributes"] = SerializeAttributeFilter(*options.AttributeFilter_);
    }
    return result;
}

TNode SerializeParamsForCopy(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    result["source_path"] = AddPathPrefix(sourcePath);
    result["destination_path"] = AddPathPrefix(destinationPath);
    result["recursive"] = options.Recursive_;
    result["force"] = options.Force_;
    result["preserve_account"] = options.PreserveAccount_;
    if (options.PreserveExpirationTime_) {
        result["preserve_expiration_time"] = *options.PreserveExpirationTime_;
    }
    return result;
}

TNode SerializeParamsForMove(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    result["source_path"] = AddPathPrefix(sourcePath);
    result["destination_path"] = AddPathPrefix(destinationPath);
    result["recursive"] = options.Recursive_;
    result["force"] = options.Force_;
    result["preserve_account"] = options.PreserveAccount_;
    if (options.PreserveExpirationTime_) {
        result["preserve_expiration_time"] = *options.PreserveExpirationTime_;
    }
    return result;
}

TNode SerializeParamsForLink(
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    result["target_path"] = AddPathPrefix(targetPath);
    result["link_path"] = AddPathPrefix(linkPath);
    result["recursive"] = options.Recursive_;
    result["ignore_existing"] = options.IgnoreExisting_;
    result["force"] = options.Force_;
    if (options.Attributes_) {
        result["attributes"] = *options.Attributes_;
    }
    return result;
}

TNode SerializeParamsForLock(
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
    result["mode"] = ::ToString(mode);
    result["waitable"] = options.Waitable_;
    if (options.AttributeKey_) {
        result["attribute_key"] = *options.AttributeKey_;
    }
    if (options.ChildKey_) {
        result["child_key"] = *options.ChildKey_;
    }
    return result;
}

TNode SerializeParametersForInsertRows(
    const TYPath& path,
    const TInsertRowsOptions& options)
{
    TNode result;
    SetPathParam(&result, path);
    if (options.Aggregate_) {
        result["aggregate"] = *options.Aggregate_;
    }
    if (options.Update_) {
        result["update"] = *options.Update_;
    }
    if (options.Atomicity_) {
        result["atomicity"] = ::ToString(*options.Atomicity_);
    }
    if (options.Durability_) {
        result["durability"] = ::ToString(*options.Durability_);
    }
    if (options.RequireSyncReplica_) {
      result["require_sync_replica"] = *options.RequireSyncReplica_;
    }
    return result;
}

TNode SerializeParametersForDeleteRows(
    const TYPath& path,
    const TDeleteRowsOptions& options)
{
    TNode result;
    SetPathParam(&result, path);
    if (options.Atomicity_) {
        result["atomicity"] = ::ToString(*options.Atomicity_);
    }
    if (options.Durability_) {
        result["durability"] = ::ToString(*options.Durability_);
    }
    if (options.RequireSyncReplica_) {
        result["require_sync_replica"] = *options.RequireSyncReplica_;
    }
    return result;
}

TNode SerializeParametersForTrimRows(
    const TYPath& path,
    const TTrimRowsOptions& /* options*/)
{
    TNode result;
    SetPathParam(&result, path);
    return result;
}

TNode SerializeParamsForParseYPath(const TRichYPath& path)
{
    TNode result;
    result["path"] = PathToNode(path);
    return result;
}

TNode SerializeParamsForAlterTableReplica(const TReplicaId& replicaId, const TAlterTableReplicaOptions& options)
{
    TNode result;
    result["replica_id"] = GetGuidAsString(replicaId);
    if (options.Enabled_) {
        result["enabled"] = *options.Enabled_;
    }
    if (options.Mode_) {
        result["mode"] = ::ToString(*options.Mode_);
    }
    return result;
}

TNode SerializeParamsForAlterTable(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
    if (options.Dynamic_) {
        result["dynamic"] = *options.Dynamic_;
    }
    if (options.Schema_) {
        TNode schema;
        {
            TNodeBuilder builder(&schema);
            Serialize(*options.Schema_, &builder);
        }
        result["schema"] = schema;
    }
    if (options.UpstreamReplicaId_) {
        result["upstream_replica_id"] = GetGuidAsString(*options.UpstreamReplicaId_);
    }
    return result;
}

} // namespace NDetail
} // namespace NYT
