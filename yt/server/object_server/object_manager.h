#pragma once

#include "public.h"

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/meta_state/mutation.h>

#include <ytlib/profiling/profiler.h>

#include <server/cell_master/public.h>

#include <server/transaction_server/public.h>

#include <server/object_server/object_manager.pb.h>

#include <server/cypress_server/public.h>

#include <server/chunk_server/chunk_tree_ref.h>

namespace NYT {
namespace NObjectServer {

// WinAPI is great.
#undef GetObject

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
    TObjectManager(
        TObjectManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    //! Registers a new type handler.
    /*!
     *  It asserts than no handler of this type is already registered.
     */
    void RegisterHandler(IObjectTypeHandlerPtr handler);

    //! Returns the handler for a given type or |nullptr| if the type is unknown.
    IObjectTypeHandlerPtr FindHandler(EObjectType type) const;

    //! Returns the handler for a given type.
    IObjectTypeHandlerPtr GetHandler(EObjectType type) const;
    
    //! Returns the handler for a given object.
    IObjectTypeHandlerPtr GetHandler(TObjectBase* object) const;

    //! Returns the cell id.
    TCellId GetCellId() const;

    //! Returns the cell unique id.
    const TGuid& GetCellGuid() const;

    //! Creates a new unique object id.
    TObjectId GenerateId(EObjectType type);

    //! Adds a reference.
    void RefObject(TObjectBase* object);
    void RefObject(NChunkServer::TChunkTreeRef ref);

    //! Removes a reference.
    void UnrefObject(TObjectBase* object);
    void UnrefObject(NChunkServer::TChunkTreeRef ref);

    //! Finds object by id, returns |nullptr| if nothing is found.
    TObjectBase* FindObject(const TObjectId& id);

    //! Finds object by id, fails if nothing is found.
    TObjectBase* GetObject(const TObjectId& id);

    //! Returns a proxy for the object with the given versioned id.
    IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction = nullptr);

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

    NMetaState::TMutationPtr CreateDestroyObjectsMutation(
        const NProto::TMetaReqDestroyObjects& request);

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

    TGarbageCollectorPtr GarbageCollector;

    NProfiling::TAggregateCounter CreatedObjectCounter;
    NProfiling::TAggregateCounter DestroyedObjectCounter;

    //! Stores deltas from parent transaction.
    NMetaState::TMetaStateMap<TVersionedObjectId, TAttributeSet> Attributes;

    void SaveKeys(const NCellMaster::TSaveContext& context) const;
    void SaveValues(const NCellMaster::TSaveContext& context) const;
    void LoadKeys(const NCellMaster::TLoadContext& context);
    void LoadValues(const NCellMaster::TLoadContext& context);
    
    virtual void OnStartRecovery() override;
    virtual void OnStopRecovery() override;
    virtual void Clear() override;

    void ReplayVerb(const NProto::TMetaReqExecute& request);

    void DestroyObjects(const NProto::TMetaReqDestroyObjects& request);
    void DestroyObject(const TObjectId& id);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

