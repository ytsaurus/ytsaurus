#pragma once

#include "public.h"
#include "attribute_set.h"
#include "type_handler.h"

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>

#include <server/cell_master/public.h>

#include <server/transaction_server/public.h>

#include <server/object_server/object_manager.pb.h>

#include <server/cypress_server/public.h>

#include <server/chunk_server/chunk_tree_ref.h>

#include <ytlib/profiling/profiler.h>

#include <queue>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides high-level management and tracking of objects and their attributes.
/*!
 *  \note
 *  Thread affinity: single-threaded
 */
class TObjectManager
    : public NMetaState::TMetaStatePart
{
public:
    //! Initializes a new instance.
    TObjectManager(
        TObjectManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();

    //! Registers a new type handler.
    /*!
     *  It asserts than no handler of this type is already registered.
     */
    void RegisterHandler(IObjectTypeHandlerPtr handler);

    //! Returns the handler for a given type or NULL if the type is unknown.
    IObjectTypeHandlerPtr FindHandler(EObjectType type) const;

    //! Returns the handler for a given type.
    IObjectTypeHandlerPtr GetHandler(EObjectType type) const;
    
    //! Returns the handler for a given id.
    IObjectTypeHandlerPtr GetHandler(const TObjectId& id) const;

    //! Returns the cell id.
    TCellId GetCellId() const;

    //! Returns the cell unique id.
    const TGuid& GetCellGuid() const;

    //! Creates a new unique object id.
    TObjectId GenerateId(EObjectType type);

    //! Adds a reference.
    void RefObject(const TObjectId& id);
    void RefObject(const TVersionedObjectId& id);
    void RefObject(TObjectWithIdBase* object);
    void RefObject(NCypressServer::ICypressNode* node);
    void RefObject(NChunkServer::TChunkTreeRef ref);

    //! Removes a reference.
    void UnrefObject(const TObjectId& id);
    void UnrefObject(const TVersionedObjectId& id);
    void UnrefObject(TObjectWithIdBase* object);
    void UnrefObject(NCypressServer::ICypressNode* node);
    void UnrefObject(NChunkServer::TChunkTreeRef ref);

    //! Returns the current reference counter.
    i32 GetObjectRefCounter(const TObjectId& id);

    //! Returns True if an object with the given #id exists.
    bool ObjectExists(const TObjectId& id);

    //! Returns a proxy for the object with the given versioned id or NULL if there's no such object.
    IObjectProxyPtr FindProxy(
        const TObjectId& id,
        NTransactionServer::TTransaction* transaction = NULL);

    //! Returns a proxy for the object with the given versioned id or NULL. Fails if there's no such object.
    IObjectProxyPtr GetProxy(
        const TObjectId& id,
        NTransactionServer::TTransaction* transaction = NULL);

    //! Creates a new empty attribute set.
    TAttributeSet* CreateAttributes(const TVersionedObjectId& id);

    //! Removes an existing attribute set.
    void RemoveAttributes(const TVersionedObjectId& id);

    //! Called when a versioned object is branched.
    void BranchAttributes(
        const TVersionedObjectId& originatingId,
        const TVersionedObjectId& branchedId);

    //! Called when a versioned object is merged during transaction commit.
    void MergeAttributes(
        const TVersionedObjectId& originatingId,
        const TVersionedObjectId& branchedId);

    //! Returns a YPath service that handles all requests.
    /*!
     *  This service supports some special prefix syntax for YPaths:
     */
    NYTree::IYPathServicePtr GetRootService();
    
    //! Executes a YPath verb, logging the change if necessary.
    /*!
     *  \param id The id of the object that handles the verb.
     *  If the change is logged, this id is written to the changelog and
     *  used afterwards to replay the change.
     *  \param isWrite True if the verb modifies the state and thus must be logged.
     *  \param context The request context.
     *  \param action An action to call that executes the actual verb logic.
     *  
     *  Note that #action takes a context as a parameter. This is because the original #context
     *  gets wrapped to intercept replies so #action gets the wrapped instance.
     */
    void ExecuteVerb(
        const TVersionedObjectId& id,
        bool isWrite,
        NRpc::IServiceContextPtr context,
        TCallback<void(NRpc::IServiceContextPtr)> action);

    //! Returns a future that gets set when the GC queues becomes empty.
    TFuture<void> GCCollect();

    DECLARE_METAMAP_ACCESSORS(Attributes, TAttributeSet, TVersionedObjectId);

private:
    typedef TObjectManager TThis;

    class TServiceContextWrapper;
    class TRootService;

    TObjectManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    std::vector<IObjectTypeHandlerPtr> TypeToHandler;
    TIntrusivePtr<TRootService> RootService;
    mutable TGuid CachedCellGuild;

    NProfiling::TAggregateCounter GCQueueSizeCounter;
    NProfiling::TAggregateCounter DestroyedObjectCounter;

    //! Stores deltas from parent transaction.
    NMetaState::TMetaStateMap<TVersionedObjectId, TAttributeSet> Attributes;

    //! Contains the ids of object have reached ref counter of 0
    //! but are not destroyed yet.
    std::deque<TObjectId> GCQueue;

    //! This promise is set each time #GCQueue becomes empty.
    TPromise<void> GCCollectPromise;

    void SaveKeys(const NCellMaster::TSaveContext& context) const;
    void SaveValues(const NCellMaster::TSaveContext& context) const;
    void LoadKeys(const NCellMaster::TLoadContext& context);
    void LoadValues(const NCellMaster::TLoadContext& context);
    
    virtual void OnStartRecovery() override;
    virtual void OnStopRecovery() override;
    virtual void Clear() override;

    void ReplayVerb(const NProto::TMetaReqExecute& request);

    void OnTransactionCommitted(NTransactionServer::TTransaction* transaction);
    void OnTransactionAborted(NTransactionServer::TTransaction* transaction);
    void PromoteCreatedObjects(NTransactionServer::TTransaction* transaction);
    void ReleaseCreatedObjects(NTransactionServer::TTransaction* transaction);

    void OnObjectReferenced(const TObjectId& id, i32 refCounter);
    void OnObjectUnreferenced(const TObjectId& id, i32 refCounter);

    void DestroyObjects(const NProto::TMetaReqDestroyObjects& request);
    void DestroyObject(const TObjectId& id);

    void GCEnqueue(const TObjectId& id);
    void GCDequeue(const TObjectId& expectedId);

    void ScheduleGCSweep();
    void GCSweep();
    void OnGCCommitSucceeded();
    void OnGCCommitFailed(const TError& error);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

