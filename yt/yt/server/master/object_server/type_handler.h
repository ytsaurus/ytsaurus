#pragma once

#include "public.h"
#include "object_proxy.h"

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/public.h>

#include <optional>

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

static constexpr int TypicalAcdCount = 1;
using TAcdList = TCompactVector<NSecurityServer::TAccessControlDescriptor*, TypicalAcdCount>;

//! Provides a bridge between TObjectManager and concrete object implementations.
struct IObjectTypeHandler
    : public virtual TRefCounted
{
    //! Returns a bunch of flags that control object replication.
    virtual ETypeFlags GetFlags() const = 0;

    //! Returns the list of tag of secondary cells where the object was replicated to.
    //! For non-replicated objects this is just the empty list.
    virtual TCellTagList GetReplicationCellTags(const TObject* object) = 0;

    //! Returns the object type managed by the handler.
    virtual EObjectType GetType() const = 0;

    //! Returns a human-readable object name.
    virtual TString GetName(const TObject* object) = 0;

    //! Returns a human-readable object path if such path exists, otherwise returns empty string.
    virtual TString GetPath(const TObject* object) = 0;

    //! Finds object by id, returns |nullptr| if nothing is found.
    virtual TObject* FindObject(TObjectId id) = 0;

    //! Finds an object by attributes intended for #CreateObject, returns |nullptr|
    //! if nothing is found and |std::nullopt| if the functionality is not supported for the type.
    virtual std::optional<TObject*> FindObjectByAttributes(
        const NYTree::IAttributeDictionary* attributes) = 0;

    //! Finds object by id, fails if nothing is found.
    TObject* GetObject(TObjectId id);

    //! Given a versioned object id, constructs a proxy for it.
    //! The object with the given id must exist.
    virtual IObjectProxyPtr GetProxy(
        TObject* object,
        NTransactionServer::TTransaction* transaction) = 0;

    //! Creates a new object instance.
    /*!
     *  If #hintId is |NullObjectId| then a new id is generated, otherwise
     *  #hintId is used.
     *
     *  Once #Create is completed, all request attributes are copied to object attributes.
     *  The handler may alter the request appropriately to control this process.
     */
    virtual TObject* CreateObject(
        TObjectId hintId,
        NYTree::IAttributeDictionary* attributes) = 0;

    //! Constructs a new instance of the type (and, unlike #CreateObject, does little else).
    virtual std::unique_ptr<TObject> InstantiateObject(TObjectId id) = 0;

    //! Raised when the strong ref-counter of the object decreases to zero.
    virtual void ZombifyObject(TObject* object) noexcept = 0;

    //! Raised when GC finally destroys the object.
    virtual void DestroyObject(TObject* object) noexcept = 0;

    //! Raised to destroy Sequoia object.
    virtual void DestroySequoiaObject(TObject* object, const NSequoiaClient::ISequoiaTransactionPtr& transaction) noexcept = 0;

    //! Invokes #object's dtor and then immediately recreates the object in-place as ghost.
    virtual void RecreateObjectAsGhost(TObject* object) noexcept = 0;

    //! Resets staging information for #object.
    /*!
     *  If #recursive is |true| then all child objects are also released.
     */
    virtual void UnstageObject(TObject* object, bool recursive) = 0;

    //! Returns the object ACD or |nullptr| if access is not controlled.
    virtual NSecurityServer::TAccessControlDescriptor* FindAcd(TObject* object) = 0;

    //! Returns a list of all ACDs associated with the object.
    /*!
     *  The list may be empty.
     *  An object needs to know all its associated ACDs in order to handle subject removal properly.
     */
    virtual TAcdList ListAcds(TObject* object) = 0;

    //! Returns the full set of columns (entities referenced by columnar ACEs)
    //! for this object, or null if this type of object has no columns.
    virtual std::optional<std::vector<TString>> ListColumns(TObject* object) = 0;

    //! Returns the object containing parent ACL.
    virtual TObject* GetParent(TObject* object) = 0;

    //! Informs #object that it has been exported (once) to cell #cellTag.
    virtual void ExportObject(
        TObject* object,
        NObjectClient::TCellTag destinationCellTag) = 0;

    //! Informs #object that #importRefCounter of its exports to cell #cellTag
    //! have been canceled.
    virtual void UnexportObject(
        TObject* object,
        NObjectClient::TCellTag destinationCellTag,
        int importRefCounter) = 0;

    //! Checks invariants for all objects of this type.
    virtual void CheckInvariants(NCellMaster::TBootstrap* bootstrap) = 0;
};

DEFINE_REFCOUNTED_TYPE(IObjectTypeHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

