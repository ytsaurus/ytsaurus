#pragma once

#include "public.h"
#include "object_proxy.h"

#include <yt/server/security_server/acl.h>

#include <yt/server/transaction_server/public.h>

#include <yt/ytlib/object_client/master_ypath.pb.h>

#include <yt/core/misc/optional.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EObjectTransactionMode,
    (Required)
    (Forbidden)
    (Optional)
);

DEFINE_ENUM(EObjectAccountMode,
    (Required)
    (Forbidden)
    (Optional)
);

DEFINE_BIT_ENUM(ETypeFlags,
    ((None)                 (0x0000))
    ((ReplicateCreate)      (0x0001)) // replicate object creation
    ((ReplicateDestroy)     (0x0002)) // replicate object destruction
    ((ReplicateAttributes)  (0x0004)) // replicate object attribute changes
    ((Creatable)            (0x0008)) // objects of this type can be created at runtime
);

// WinAPI is great.
#undef GetObject

//! Provides a bridge between TObjectManager and concrete object implementations.
struct IObjectTypeHandler
    : public virtual TRefCounted
{
    //! Returns a bunch of flags that control object replication.
    virtual ETypeFlags GetFlags() const = 0;

    //! Returns the list of tag of secondary cells where the object was replicated to.
    //! For non-replicated objects this is just the empty list.
    virtual TCellTagList GetReplicationCellTags(const TObjectBase* object) = 0;

    //! Returns the object type managed by the handler.
    virtual EObjectType GetType() const = 0;

    //! Returns a human-readable object name.
    virtual TString GetName(const TObjectBase* object) = 0;

    //! Finds object by id, returns |nullptr| if nothing is found.
    virtual TObjectBase* FindObject(TObjectId id) = 0;

    //! Finds object by id, fails if nothing is found.
    TObjectBase* GetObject(TObjectId id);

    //! Given a versioned object id, constructs a proxy for it.
    //! The object with the given id must exist.
    virtual IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction) = 0;

    //! Creates a new object instance.
    /*!
     *  If #hintId is |NullObjectId| then a new id is generated, otherwise
     *  #hintId is used.
     *
     *  Once #Create is completed, all request attributes are copied to object attributes.
     *  The handler may alter the request appropriately to control this process.
     */
    virtual TObjectBase* CreateObject(
        TObjectId hintId,
        NYTree::IAttributeDictionary* attributes) = 0;

    //! Constructs a new instance of the type (and, unlike #CreateObject, does little else).
    virtual std::unique_ptr<TObjectBase> InstantiateObject(TObjectId id) = 0;

    //! Raised when the strong ref-counter of the object decreases to zero.
    virtual void ZombifyObject(TObjectBase* object) noexcept = 0;

    //! Raised when GC finally destroys the object.
    virtual void DestroyObject(TObjectBase* object) noexcept = 0;

    //! Resets staging information for #object.
    /*!
     *  If #recursive is |true| then all child objects are also released.
     */
    virtual void UnstageObject(TObjectBase* object, bool recursive) = 0;

    //! Returns the object ACD or |nullptr| if access is not controlled.
    virtual NSecurityServer::TAccessControlDescriptor* FindAcd(TObjectBase* object) = 0;

    //! Returns the object containing parent ACL.
    virtual TObjectBase* GetParent(TObjectBase* object) = 0;

    //! Informs #object that it has been exported (once) to cell #cellTag.
    virtual void ExportObject(
        TObjectBase* object,
        NObjectClient::TCellTag destinationCellTag) = 0;

    //! Informs #object that #importRefCounter of its exports to cell #cellTag
    //! have been canceled.
    virtual void UnexportObject(
        TObjectBase* object,
        NObjectClient::TCellTag destinationCellTag,
        int importRefCounter) = 0;

};

DEFINE_REFCOUNTED_TYPE(IObjectTypeHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

