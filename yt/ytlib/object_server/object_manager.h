#pragma once

#include "common.h"
#include "type_handler.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/meta_state/composite_meta_state.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides high-level management and tracking of objects.
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

    //! Registers a type handler.
    /*!
     *  It asserts than no handler of this type is already registered.
     */
    void RegisterHandler(IObjectTypeHandler* handler);

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

    //! Returns a proxy for the object with the given id or NULL if there's no such object.
    IObjectProxy::TPtr FindProxy(const TObjectId& id);

    //! Returns a proxy for the object with the given id or NULL. Fails if there's no such object.
    IObjectProxy::TPtr GetProxy(const TObjectId& id);

private:
    TCellId CellId;
    ui64 Counter;

    IObjectTypeHandler::TPtr TypeToHandler[MaxObjectType];

    TFuture<TVoid>::TPtr Save(const NMetaState::TCompositeMetaState::TSaveContext& context);
    void Load(TInputStream* input);
    virtual void Clear();

    DECLARE_THREAD_AFFINITY_SLOT(StateThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

