#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(ISequoiaAttributeFetcher)

struct TRequestExecutedPayload
{ };

struct TForwardToMasterPayload
{
    std::optional<NYson::TYsonString> EffectiveAcl;
};

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
    NSequoiaClient::TAbsolutePath Path;
};

struct TCypressChildDescriptor
{
    NCypressClient::TNodeId ParentId;
    NCypressClient::TNodeId ChildId;
    std::string ChildKey;
};

struct TAccessControlDescriptor
{
    NCypressClient::TNodeId NodeId;
    NSecurityClient::TSerializableAccessControlList Acl;
    bool Inherit = false;
};

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, CypressProxyLogger, "CypressProxy");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, CypressProxyProfiler, "/cypress_proxy");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
