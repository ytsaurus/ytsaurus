#pragma once

#include "public.h"

#include <yt/server/scheduler/scheduling_tag.h>

#include <yt/client/api/public.h>

#include <yt/ytlib/controller_agent/controller_agent_service.pb.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TIntrinsicRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, Id);
    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, Spec);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYVAL_RO_PROPERTY(TString, AuthenticatedUser);
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, SecureVault);
    DEFINE_BYVAL_RO_PROPERTY(std::vector<TString>, Owners);
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::TTransactionId, UserTransactionId);
    DEFINE_BYREF_RO_PROPERTY(NScheduler::TPoolTreeToSchedulingTagFilter, PoolTreeToSchedulingTagFilter);
    DEFINE_BYVAL_RW_PROPERTY(TMemoryTag, MemoryTag);
    DEFINE_BYVAL_RW_PROPERTY(std::vector<NTransactionClient::TTransactionId>, WatchTransactionIds);
    DEFINE_BYVAL_RW_PROPERTY(IOperationControllerPtr, Controller);
    DEFINE_BYVAL_RW_PROPERTY(TOperationControllerHostPtr, Host);

public:
    explicit TOperation(const NProto::TOperationDescriptor& descriptor);

    const IOperationControllerPtr& GetControllerOrThrow() const;
};

DEFINE_REFCOUNTED_TYPE(TOperation)

} // namespace NControllerAgent

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NControllerAgent::NProto::TOperationDescriptor::TPoolTreeSchedulingTagFilters* protoTreeFilters,
    const NScheduler::TPoolTreeToSchedulingTagFilter& treeFilters);

void FromProto(
    NScheduler::TPoolTreeToSchedulingTagFilter* treeFilters,
    const NControllerAgent::NProto::TOperationDescriptor::TPoolTreeSchedulingTagFilters protoTreeFilters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

