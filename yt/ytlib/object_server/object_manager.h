#pragma once

#include "common.h"
#include "attribute_set.h"
#include "type_handler.h"
#include "object_manager.pb.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/id_generator.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>

namespace NYT {

// TODO(babenko): killme
namespace NCypress {
    class TCypressManager;
}

namespace NTransactionServer {
    class TTransactionManager;
}

namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides high-level management and tracking of objects and their attributes.
/*!
 *  This class provides the following types of operations:
 *  
 *  - #RegisterHandler and #GetHandler are used to register object type handlers
 *  and query for them.
 *  
 *  - #GenerateId creates a new object id, which is guaranteed to be globally
 *  unique.
 *  
 *  - #RefObject and #UnrefObject add and remove references between objects, respectively.
 *  Note that "fromId" argument is used mostly for diagnostics.
 *  Additional overloads without this argument are also provided.
 *  
 *  - #GetObjectRefCounter queries the current reference counter.
 *  
 *  \note
 *  Thread affinity: single-threaded
 *
 */
class TObjectManager
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TObjectManager> TPtr;

    //! Initializes a new instance.
    TObjectManager(
        NMetaState::IMetaStateManager* metaStateManager,
        NMetaState::TCompositeMetaState* metaState,
        TCellId cellId);

    ~TObjectManager();

    // TODO: killme
    void SetCypressManager(NCypress::TCypressManager* cypressManager);
    void SetTransactionManager(NTransactionServer::TTransactionManager* transactionmanager);

    //! Registers a type handler.
    /*!
     *  It asserts than no handler of this type is already registered.
     */
    void RegisterHandler(IObjectTypeHandler* handler);

    //! Returns the handler for a given type or NULL if the type is unknown.
    IObjectTypeHandler* FindHandler(EObjectType type) const;

    //! Returns the handler for a given type.
    IObjectTypeHandler* GetHandler(EObjectType type) const;
    
    //! Returns the handler for a given id.
    IObjectTypeHandler* GetHandler(const TObjectId& id) const;

    //! Returns the cell id.
    TCellId GetCellId() const;

    //! Creates a new unique object id.
    TObjectId GenerateId(EObjectType type);

    //! Adds a reference.
    void RefObject(const TObjectId& id);

    //! Removes a reference (from an unspecified source).
    void UnrefObject(const TObjectId& id);

    //! Returns the current reference counter.
    i32 GetObjectRefCounter(const TObjectId& id);

    //! Returns a proxy for the object with the given versioned id or NULL if there's no such object.
    IObjectProxy::TPtr FindProxy(const TVersionedObjectId& id);

    //! Returns a proxy for the object with the given versioned id or NULL. Fails if there's no such object.
    IObjectProxy::TPtr GetProxy(const TVersionedObjectId& id);

    TAttributeSet* CreateAttributes(const TVersionedObjectId& id);
    void RemoveAttributes(const TVersionedObjectId& id);

    //! Called on a versioned object is branched.
    void BranchAttributes(
        const TVersionedObjectId& originatingId,
        const TVersionedObjectId& branchedId);

    //! Called when a versioned object is merged during transaction commit.
    void MergeAttributes(
        const TVersionedObjectId& originatingId,
        const TVersionedObjectId& branchedId);

    NYTree::IYPathService* GetRootService();
    
    void ExecuteVerb(
        const TVersionedObjectId& id,
        bool isWrite,
        NRpc::IServiceContext* context,
        IParamAction<NRpc::IServiceContext*>* action);

    DECLARE_METAMAP_ACCESSORS(Attributes, TAttributeSet, TVersionedObjectId);

private:
    class TServiceContextWrapper;
    class TRootService;

    TCellId CellId;
    yvector< TIdGenerator<ui64> > TypeToCounter;
    yvector<IObjectTypeHandler::TPtr> TypeToHandler;
    TIntrusivePtr<TRootService> RootService;

    TIntrusivePtr<NCypress::TCypressManager> CypressManager;
    TIntrusivePtr<NTransactionServer::TTransactionManager> TransactionManager;

    // Stores deltas from parent transaction.
    NMetaState::TMetaStateMap<TVersionedObjectId, TAttributeSet> Attributes;

    TFuture<TVoid>::TPtr Save(const NMetaState::TCompositeMetaState::TSaveContext& context);
    void Load(TInputStream* input);
    virtual void Clear();

    TVoid ReplayVerb(const NProto::TMsgExecuteVerb& message);

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

