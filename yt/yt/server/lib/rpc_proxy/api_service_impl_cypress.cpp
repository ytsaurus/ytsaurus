#include "api_service_impl.h"

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterCypressMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ExistsNode));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetNode));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ListNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(CreateNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(RemoveNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(SetNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(MultisetAttributesNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(LockNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(UnlockNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(CopyNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(MoveNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(LinkNode));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(ConcatenateNodes));

    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(CreateObject));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, CreateObject)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto type = FromProto<EObjectType>(request->type());
    TCreateObjectOptions options;
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_ignore_existing()) {
        options.IgnoreExisting = request->ignore_existing();
    }
    if (request->has_attributes()) {
        options.Attributes = NYTree::FromProto(request->attributes());
    }

    context->AnnotateRequest()
        .With("Type", type)
        .With("IgnoreExisting", options.IgnoreExisting);

    ExecuteCall(
        context,
        [=] {
            return client->CreateObject(type, options);
        },
        [] (const auto& context, NObjectClient::TObjectId objectId) {
            auto* response = &context->Response();
            ToProto(response->mutable_object_id(), objectId);

            context->AnnotateResponse()
                .With("ObjectId", objectId);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ExistsNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TNodeExistsOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }
    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->NodeExists(path, options);
        },
        [] (const auto& context, const bool& result) {
            auto* response = &context->Response();
            response->set_exists(result);

            context->AnnotateResponse()
                .With("Exists", result);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TGetNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_attributes()) {
        FromProto(&options.Attributes, request->attributes());
    } else if (request->has_legacy_attributes() && !request->legacy_attributes().all()) {
        // COMPAT(max42): remove when no clients older than Aug22 are there.
        options.Attributes = TAttributeFilter(FromProto<std::vector<std::string>>(request->legacy_attributes().keys()));
    }
    if (request->has_max_size()) {
        options.MaxSize = request->max_size();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }
    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }
    if (request->has_complexity_limits()) {
        FromProto(&options.ComplexityLimits, request->complexity_limits());
    }
    if (request->has_options()) {
        options.Options = NYTree::FromProto(request->options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("AttributeFilter", options.Attributes);

    ExecuteCall(
        context,
        [=] {
            return client->GetNode(path, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_value(ToProto(result));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ListNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TListNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_attributes()) {
        FromProto(&options.Attributes, request->attributes());
    } else if (request->has_legacy_attributes() && !request->legacy_attributes().all()) {
        // COMPAT(max42): remove when no clients older than Aug22 are there.
        options.Attributes = TAttributeFilter(FromProto<std::vector<std::string>>(request->legacy_attributes().keys()));
    }
    if (request->has_max_size()) {
        options.MaxSize = request->max_size();
    }
    if (request->has_complexity_limits()) {
        FromProto(&options.ComplexityLimits, request->complexity_limits());
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }
    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("AttributeFilter", options.Attributes);

    ExecuteCall(
        context,
        [=] {
            return client->ListNode(path, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_value(ToProto(result));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CreateNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    auto type = FromProto<NObjectClient::EObjectType>(request->type());

    TCreateNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_attributes()) {
        options.Attributes = NYTree::FromProto(request->attributes());
    }
    if (request->has_recursive()) {
        options.Recursive = request->recursive();
    }
    if (request->has_force()) {
        options.Force = request->force();
    }
    if (request->has_ignore_existing()) {
        options.IgnoreExisting = request->ignore_existing();
    }
    if (request->has_lock_existing()) {
        options.LockExisting = request->lock_existing();
    }
    if (request->has_ignore_type_mismatch()) {
        options.IgnoreTypeMismatch = request->ignore_type_mismatch();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("Type", type);

    ExecuteCall(
        context,
        [=] {
            return client->CreateNode(path, type, options);
        },
        [] (const auto& context, NCypressClient::TNodeId nodeId) {
            auto* response = &context->Response();
            ToProto(response->mutable_node_id(), nodeId);

            context->AnnotateResponse()
                .With("NodeId", nodeId);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, RemoveNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TRemoveNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_recursive()) {
        options.Recursive = request->recursive();
    }
    if (request->has_force()) {
        options.Force = request->force();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->RemoveNode(path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, SetNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    auto value = TYsonString(request->value());

    TSetNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_recursive()) {
        options.Recursive = request->recursive();
    }
    if (request->has_force()) {
        options.Force = request->force();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->SetNode(path, value, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, MultisetAttributesNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    auto attributes = GetEphemeralNodeFactory()->CreateMap();
    for (const auto& protoSubrequest : request->subrequests()) {
        attributes->AddChild(
            protoSubrequest.attribute(),
            ConvertToNode(TYsonString(protoSubrequest.value())));
    }

    TMultisetAttributesNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_force()) {
        options.Force = request->force();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("Attributes", attributes->GetKeys());

    ExecuteCall(
        context,
        [=] {
            return client->MultisetAttributesNode(path, attributes, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, LockNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    auto mode = FromProto<NCypressClient::ELockMode>(request->mode());

    TLockNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_waitable()) {
        options.Waitable = request->waitable();
    }
    if (request->has_child_key()) {
        options.ChildKey = request->child_key();
    }
    if (request->has_attribute_key()) {
        options.AttributeKey = request->attribute_key();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("Mode", mode);

    ExecuteCall(
        context,
        [=] {
            return client->LockNode(path, mode, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_node_id(), result.NodeId);
            ToProto(response->mutable_lock_id(), result.LockId);
            response->set_revision(ToProto(result.Revision));

            context->AnnotateResponse()
                .With("NodeId", result.NodeId)
                .With("LockId", result.LockId)
                .WithFormat("Revision", "%x", result.Revision);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, UnlockNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TUnlockNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->UnlockNode(path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CopyNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& srcPath = request->src_path();
    const auto& dstPath = request->dst_path();

    TCopyNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_recursive()) {
        options.Recursive = request->recursive();
    }
    if (request->has_ignore_existing()) {
        options.IgnoreExisting = request->ignore_existing();
    }
    if (request->has_lock_existing()) {
        options.LockExisting = request->lock_existing();
    }
    if (request->has_force()) {
        options.Force = request->force();
    }
    if (request->has_preserve_account()) {
        options.PreserveAccount = request->preserve_account();
    }
    if (request->has_preserve_creation_time()) {
        options.PreserveCreationTime = request->preserve_creation_time();
    }
    if (request->has_preserve_modification_time()) {
        options.PreserveModificationTime = request->preserve_modification_time();
    }
    if (request->has_preserve_expiration_time()) {
        options.PreserveExpirationTime = request->preserve_expiration_time();
    }
    if (request->has_preserve_expiration_timeout()) {
        options.PreserveExpirationTimeout = request->preserve_expiration_timeout();
    }
    if (request->has_preserve_owner()) {
        options.PreserveOwner = request->preserve_owner();
    }
    if (request->has_preserve_acl()) {
        options.PreserveAcl = request->preserve_acl();
    }
    if (request->has_pessimistic_quota_check()) {
        options.PessimisticQuotaCheck = request->pessimistic_quota_check();
    }
    if (request->has_enable_cross_cell_copying()) {
        options.EnableCrossCellCopying = request->enable_cross_cell_copying();
    }
    if (request->has_allow_secondary_index_abandonment()) {
        options.AllowSecondaryIndexAbandonment = request->allow_secondary_index_abandonment();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("SrcPath", srcPath)
        .With("DstPath", dstPath);

    ExecuteCall(
        context,
        [=] {
            return client->CopyNode(srcPath, dstPath, options);
        },
        [] (const auto& context, NCypressClient::TNodeId nodeId) {
            auto* response = &context->Response();
            ToProto(response->mutable_node_id(), nodeId);

            context->AnnotateResponse()
                .With("NodeId", nodeId);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, MoveNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& srcPath = request->src_path();
    const auto& dstPath = request->dst_path();

    TMoveNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_recursive()) {
        options.Recursive = request->recursive();
    }
    if (request->has_force()) {
        options.Force = request->force();
    }
    if (request->has_preserve_account()) {
        options.PreserveAccount = request->preserve_account();
    }
    if (request->has_preserve_creation_time()) {
        options.PreserveCreationTime = request->preserve_creation_time();
    }
    if (request->has_preserve_modification_time()) {
        options.PreserveModificationTime = request->preserve_modification_time();
    }
    if (request->has_preserve_expiration_time()) {
        options.PreserveExpirationTime = request->preserve_expiration_time();
    }
    if (request->has_preserve_expiration_timeout()) {
        options.PreserveExpirationTimeout = request->preserve_expiration_timeout();
    }
    if (request->has_preserve_owner()) {
        options.PreserveOwner = request->preserve_owner();
    }
    if (request->has_preserve_acl()) {
        options.PreserveAcl = request->preserve_acl();
    }
    if (request->has_pessimistic_quota_check()) {
        options.PessimisticQuotaCheck = request->pessimistic_quota_check();
    }
    if (request->has_enable_cross_cell_copying()) {
        options.EnableCrossCellCopying = request->enable_cross_cell_copying();
    }
    if (request->has_allow_secondary_index_abandonment()) {
        options.AllowSecondaryIndexAbandonment = request->allow_secondary_index_abandonment();
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("SrcPath", srcPath)
        .With("DstPath", dstPath);

    ExecuteCall(
        context,
        [=] {
            return client->MoveNode(srcPath, dstPath, options);
        },
        [] (const auto& context, const auto& nodeId) {
            auto* response = &context->Response();
            ToProto(response->mutable_node_id(), nodeId);

            context->AnnotateResponse()
                .With("NodeId", nodeId);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, LinkNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& srcPath = request->src_path();
    const auto& dstPath = request->dst_path();

    TLinkNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_recursive()) {
        options.Recursive = request->recursive();
    }
    if (request->has_force()) {
        options.Force = request->force();
    }
    if (request->has_ignore_existing()) {
        options.IgnoreExisting = request->ignore_existing();
    }
    if (request->has_lock_existing()) {
        options.LockExisting = request->lock_existing();
    }
    if (request->has_attributes()) {
        options.Attributes = NYTree::FromProto(request->attributes());
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("SrcPath", srcPath)
        .With("DstPath", dstPath);

    ExecuteCall(
        context,
        [=] {
            return client->LinkNode(
            srcPath,
            dstPath,
            options);
        },
        [] (const auto& context, const auto& nodeId) {
            auto* response = &context->Response();
            ToProto(response->mutable_node_id(), nodeId);

            context->AnnotateResponse()
                .With("NodeId", nodeId);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ConcatenateNodes)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto srcPaths = FromProto<std::vector<TRichYPath>>(request->src_paths());
    auto dstPath = FromProto<TRichYPath>(request->dst_path());

    TConcatenateNodesOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }

    options.ChunkMetaFetcherConfig = New<NChunkClient::TFetcherConfig>();

    context->AnnotateRequest()
        .With("SrcPaths", srcPaths)
        .With("DstPath", dstPath);

    if (request->has_fetcher()) {
        options.ChunkMetaFetcherConfig->NodeRpcTimeout = FromProto<TDuration>(request->fetcher().node_rpc_timeout());
    }

    ExecuteCall(
        context,
        [=] {
            return client->ConcatenateNodes(srcPaths, dstPath, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ExternalizeNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    auto cellTag = FromProto<TCellTag>(request->cell_tag());

    TExternalizeNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }

    context->AnnotateRequest()
        .With("Path", path)
        .With("CellTag", cellTag);

    ExecuteCall(
        context,
        [=] {
            return client->ExternalizeNode(path, cellTag, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, InternalizeNode)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TInternalizeNodeOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->InternalizeNode(path, options);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
