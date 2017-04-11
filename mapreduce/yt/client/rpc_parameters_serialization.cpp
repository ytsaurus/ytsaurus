#include "rpc_parameters_serialization.h"

#include <mapreduce/yt/common/helpers.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/client_method_options.h>
#include <mapreduce/yt/interface/node.h>

#include <util/generic/guid.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////

Stroka ToString(ELockMode mode)
{
    switch (mode) {
        case LM_EXCLUSIVE: return "exclusive";
        case LM_SHARED: return "shared";
        case LM_SNAPSHOT: return "snapshot";
        default:
            ythrow yexception() << "Invalid lock mode " << static_cast<int>(mode);
    }
}

Stroka ToString(ENodeType type)
{
    switch (type) {
        case NT_STRING: return "string_node";
        case NT_INT64: return "int64_node";
        case NT_UINT64: return "uint64_node";
        case NT_DOUBLE: return "double_node";
        case NT_BOOLEAN: return "boolean_node";
        case NT_MAP: return "map_node";
        case NT_LIST: return "list_node";
        case NT_FILE: return "file";
        case NT_TABLE: return "table";
        case NT_DOCUMENT: return "document";
        default:
            ythrow yexception() << "Invalid node type " << static_cast<int>(type);
    }
}

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
    result["type"] = ToString(type);
    result["ignore_existing"] = options.IgnoreExisting_;
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
    if (options.IgnoreOpaque_) {
        result["ignore_opaque"] = options.IgnoreOpaque_;
    }
    return result;
}

TNode SerializeParamsForSet(
    const TTransactionId& transactionId,
    const TYPath& path)
{
    TNode result;
    SetTransactionIdParam(&result, transactionId);
    SetPathParam(&result, path);
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
    result["mode"] = ToString(mode);
    result["waitable"] = options.Waitable_;
    if (options.AttributeKey_) {
        result["attribute_key"] = *options.AttributeKey_;
    }
    if (options.ChildKey_) {
        result["child_key"] = *options.ChildKey_;
    }
    return result;
}

} // namespace NDetail
} // namespace NYT
