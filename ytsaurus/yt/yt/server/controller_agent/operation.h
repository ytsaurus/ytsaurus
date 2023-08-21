#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/experiments.h>
#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, Id);
    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, Spec);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYVAL_RO_PROPERTY(TString, AuthenticatedUser);
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, SecureVault);
    DEFINE_BYVAL_RW_PROPERTY(NSecurityClient::TSerializableAccessControlList, Acl);
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::TTransactionId, UserTransactionId);
    DEFINE_BYREF_RO_PROPERTY(NScheduler::TPoolTreeControllerSettingsMap, PoolTreeControllerSettingsMap);
    DEFINE_BYVAL_RO_PROPERTY(NScheduler::TControllerEpoch, ControllerEpoch);
    DEFINE_BYVAL_RW_PROPERTY(TMemoryTag, MemoryTag);
    DEFINE_BYVAL_RW_PROPERTY(std::vector<NTransactionClient::TTransactionId>, WatchTransactionIds);
    DEFINE_BYVAL_RW_PROPERTY(IOperationControllerPtr, Controller);
    DEFINE_BYVAL_RW_PROPERTY(IOperationControllerHostPtr, Host);
    DEFINE_BYREF_RO_PROPERTY(std::vector<NScheduler::TExperimentAssignmentPtr>, ExperimentAssignments);

public:
    explicit TOperation(const NProto::TOperationDescriptor& descriptor);

    const IOperationControllerPtr& GetControllerOrThrow() const;
};

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
