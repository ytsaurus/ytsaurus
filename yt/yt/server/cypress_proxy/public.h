#pragma once

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap;

DECLARE_REFCOUNTED_STRUCT(ISequoiaServiceContext)
DECLARE_REFCOUNTED_CLASS(TSequoiaServiceContextWrapper)

template <class RequestMessage, class ResponseMessage>
using TTypedSequoiaServiceContext = NRpc::TGenericTypedServiceContext<
    ISequoiaServiceContext,
    TSequoiaServiceContextWrapper,
    RequestMessage,
    ResponseMessage>;

DECLARE_REFCOUNTED_CLASS(TNodeProxyBase)

DECLARE_REFCOUNTED_STRUCT(IObjectService)
DECLARE_REFCOUNTED_STRUCT(ISequoiaService)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(IUserDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(TUserDirectory)

DECLARE_REFCOUNTED_CLASS(TPerUserAndWorkloadRequestQueueProvider);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TObjectServiceDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TSequoiaResponseKeeperDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCypressProxyDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TUserDirectorySynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSequoiaSession)

DECLARE_REFCOUNTED_STRUCT(ISequoiaResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

struct TCypressResolveResult;
struct TSequoiaResolveResult;

using TResolveResult = std::variant<
    TCypressResolveResult,
    TSequoiaResolveResult
>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUserWorkloadType,
    (Read)
    (Write)
);

////////////////////////////////////////////////////////////////////////////////

struct TCopyOptions
{
    NCypressClient::ENodeCloneMode Mode = NCypressClient::ENodeCloneMode::Copy;
    bool PreserveAcl = false;
    bool PreserveAccount = false;
    bool PreserveOwner = false;
    bool PreserveCreationTime = false;
    bool PreserveModificationTime = false;
    bool PreserveExpirationTime = false;
    bool PreserveExpirationTimeout = false;
    bool PessimisticQuotaCheck = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TMultisetAttributesSubrequest
{
    std::string AttributeKey;
    NYson::TYsonString Value;
};

////////////////////////////////////////////////////////////////////////////////

struct TCypressNodeDescriptor
{
    NCypressClient::TNodeId Id;
    NSequoiaClient::TAbsoluteYPath Path;
};

struct TCypressChildDescriptor
{
    NCypressClient::TNodeId ParentId;
    NCypressClient::TNodeId ChildId;
    std::string ChildKey;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
