#pragma once

#include "persistence.h"

#include <yt/ytlib/api/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/ypath/public.h>

#include <yt/core/misc/variant.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yp/client/api/proto/object_service.pb.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionCommitResult
{
    TTimestamp CommitTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

struct TSetUpdateRequest
{
    NYT::NYPath::TYPath Path;
    NYT::NYTree::INodePtr Value;
    bool Recursive = false;
};

void FromProto(TSetUpdateRequest* request, const NClient::NApi::NProto::TSetUpdate& protoRequest);

struct TRemoveUpdateRequest
{
    NYT::NYPath::TYPath Path;
};

void FromProto(TRemoveUpdateRequest* request, const NClient::NApi::NProto::TRemoveUpdate& protoRequest);

using TUpdateRequest = NYT::TVariant<
    TSetUpdateRequest,
    TRemoveUpdateRequest
>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    (Active)
    (Committing)
    (Committed)
    (Failed)
    (Aborted)
);

////////////////////////////////////////////////////////////////////////////////

struct IUpdateContext
{
    virtual ~IUpdateContext() = default;

    virtual void AddSetter(std::function<void()> setter) = 0;
    virtual void AddFinalizer(std::function<void()> finalizer) = 0;

    virtual void Commit() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TAttributeValueList
{
    std::vector<NYT::NYson::TYsonString> Values;
};

////////////////////////////////////////////////////////////////////////////////

struct TAttributeSelector
{
    std::vector<NYT::NYPath::TYPath> Paths;
};

TString ToString(const TAttributeSelector& selector);

////////////////////////////////////////////////////////////////////////////////

struct TObjectFilter
{
    TString Query;
};

TString ToString(const TObjectFilter& filter);

////////////////////////////////////////////////////////////////////////////////

struct TGetQueryResult
{
    TNullable<TAttributeValueList> Object;
};

////////////////////////////////////////////////////////////////////////////////

struct TSelectQueryOptions
{
    TNullable<i64> Offset;
    TNullable<i64> Limit;
};

struct TSelectQueryResult
{
    std::vector<TAttributeValueList> Objects;
};

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NYT::TRefCounted
{
public:
    TTransaction(
        NMaster::TBootstrap* bootstrap,
        TTransactionManagerConfigPtr config,
        const TTransactionId& id,
        TTimestamp startTimestamp,
        NYT::NApi::IClientPtr client,
        NYT::NApi::ITransactionPtr underlyingTransaction);

    ETransactionState GetState() const;

    const TTransactionId& GetId() const;
    TTimestamp GetStartTimestamp() const;

    ISession* GetSession();

    std::unique_ptr<IUpdateContext> CreateUpdateContext();

    TObject* CreateObject(
        EObjectType type,
        const NYT::NYTree::IMapNodePtr& attributes);
    TObject* CreateObject(
        EObjectType type,
        const NYT::NYTree::IMapNodePtr& attributes,
        IUpdateContext* context);

    void RemoveObject(TObject* object);
    void RemoveObject(TObject* object, IUpdateContext* context);

    void UpdateObject(
        TObject* object,
        const std::vector<TUpdateRequest>& requests);
    void UpdateObject(
        TObject* object,
        const std::vector<TUpdateRequest>& requests,
        IUpdateContext* context);

    TGetQueryResult ExecuteGetQuery(
        EObjectType type,
        const TObjectId& id,
        const TAttributeSelector& selector);
    TSelectQueryResult ExecuteSelectQuery(
        EObjectType type,
        const TNullable<TObjectFilter>& filter,
        const TAttributeSelector& selector,
        const TSelectQueryOptions& options);

    TObject* GetObject(
        EObjectType type,
        const TObjectId& id,
        const TObjectId& parentId = {});

    TNode* GetNode(const TObjectId& id);
    TNode* CreateNode(const TObjectId& id = TObjectId());

    TPod* GetPod(const TObjectId& id);

    TPodSet* GetPodSet(const TObjectId& id);

    TResource* GetResource(const TObjectId& id);

    TNetworkProject* GetNetworkProject(const TObjectId& id);

    TFuture<TTransactionCommitResult> Commit();
    void Abort();

    void ScheduleNotifyAgent(TNode* node);
    void ScheduleAllocateResources(TPod* pod);
    void ScheduleValidateNodeResources(TNode* node);
    void ScheduleUpdatePodAddresses(TPod* pod);
    void ScheduleUpdatePodSpec(TPod* pod);

    NYT::NConcurrency::TAsyncSemaphoreGuard AcquireLock();

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
