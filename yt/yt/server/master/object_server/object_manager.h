#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/proto/object_manager.pb.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/test_framework/testing_tag.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides high-level management and tracking of objects.
/*!
 *  \note
 *  Thread affinity: single-threaded
 */
struct IObjectManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    //! Registers a new type handler.
    virtual void RegisterHandler(IObjectTypeHandlerPtr handler) = 0;

    //! Returns the handler for a given type or |nullptr| if the type is unknown.
    virtual const IObjectTypeHandlerPtr& FindHandler(EObjectType type) const = 0;

    //! Returns the handler for a given type. Fails if type is unknown.
    virtual const IObjectTypeHandlerPtr& GetHandler(EObjectType type) const = 0;

    //! Returns the handler for a given type. Throws if type is unknown.
    virtual const IObjectTypeHandlerPtr& GetHandlerOrThrow(EObjectType type) const = 0;

    //! Returns the handler for a given object.
    virtual const IObjectTypeHandlerPtr& GetHandler(const TObject* object) const = 0;

    //! Returns the set of registered object types, excluding schemas.
    virtual const std::set<EObjectType>& GetRegisteredTypes() const = 0;

    //! If |hintId| is |NullObjectId| then creates a new unique object id.
    //! Otherwise returns |hintId| (but checks its type).
    virtual TObjectId GenerateId(EObjectType type, TObjectId hintId = NullObjectId) = 0;

    //! Adds a reference.
    //! Returns the strong reference counter.
    virtual int RefObject(TObject* object) = 0;

    //! Removes #count references.
    //! Returns the strong reference counter.
    virtual int UnrefObject(TObject* object, int count = 1) = 0;

    //! Increments the object ephemeral reference counter thus temporarily preventing it from being destroyed.
    //! Returns the ephemeral reference counter.
    virtual int EphemeralRefObject(TObject* object) = 0;

    //! Decrements the object ephemeral reference counter thus making it eligible for destruction.
    /*
     * \note Thread affinity: Automaton or LocalRead
     */
    virtual void EphemeralUnrefObject(TObject* object) = 0;

    //! Decrements the object ephemeral reference counter thus making it eligible for destruction.
    /*
     * \note Thread affinity: any
     */
    virtual void EphemeralUnrefObject(TObject* object, TEpoch epoch) = 0;

    //! Increments the object weak reference counter thus temporarily preventing it from being destroyed.
    //! Returns the weak reference counter.
    virtual int WeakRefObject(TObject* object) = 0;

    //! Decrements the object weak reference counter thus making it eligible for destruction.
    //! Returns the weak reference counter.
    virtual int WeakUnrefObject(TObject* object) = 0;

    //! Finds object by id, returns |nullptr| if nothing is found.
    virtual TObject* FindObject(TObjectId id) = 0;

    //! Finds object by type and attributes, returns |nullptr| if nothing is found and
    //! |std::nullopt| if the functionality is not supported for the type.
    virtual std::optional<TObject*> FindObjectByAttributes(
        EObjectType type,
        const NYTree::IAttributeDictionary* attributes) = 0;

    //! Finds object by id, fails if nothing is found.
    virtual TObject* GetObject(TObjectId id) = 0;

    //! Finds object by id, throws if nothing is found.
    virtual TObject* GetObjectOrThrow(TObjectId id) = 0;

    //! Finds weak ghost object by id, fails if nothing is found.
    virtual TObject* GetWeakGhostObject(TObjectId id) = 0;

    //! For object types requiring two-phase removal, initiates the removal protocol.
    //! For others, checks for the local reference counter and if it's 1, drops the last reference.
    virtual void RemoveObject(TObject* object) = 0;

    //! Creates a cross-cell proxy for the object with the given #id.
    virtual NYTree::IYPathServicePtr CreateRemoteProxy(TObjectId id) = 0;

    //! Creates a cross-cell proxy to forward the request to a given master cell.
    virtual NYTree::IYPathServicePtr CreateRemoteProxy(TCellTag cellTag) = 0;

    //! Returns a proxy for the object with the given versioned id.
    virtual IObjectProxyPtr GetProxy(
        TObject* object,
        NTransactionServer::TTransaction* transaction = nullptr) = 0;

    //! Called when a versioned object is branched.
    virtual void BranchAttributes(
        const TObject* originatingObject,
        TObject* branchedObject) = 0;

    //! Called when a versioned object is merged during transaction commit.
    virtual void MergeAttributes(
        TObject* originatingObject,
        const TObject* branchedObject) = 0;

    //! Fills the attributes of a given unversioned object.
    virtual void FillAttributes(
        TObject* object,
        const NYTree::IAttributeDictionary& attributes) = 0;

    //! Returns a YPath service that routes all incoming requests.
    virtual NYTree::IYPathServicePtr GetRootService() = 0;

    //! Returns "master" object for handling requests sent via TMasterYPathProxy.
    virtual TObject* GetMasterObject() = 0;

    //! Returns a proxy for master object.
    /*!
     *  \see GetMasterObject
     */
    virtual IObjectProxyPtr GetMasterProxy() = 0;

    //! Finds a schema object for a given type, returns |nullptr| if nothing is found.
    virtual TObject* FindSchema(EObjectType type) = 0;

    //! Finds a schema object for a given type, fails if nothing is found.
    virtual TObject* GetSchema(EObjectType type) = 0;

    //! Returns a proxy for schema object.
    /*!
     *  \see GetSchema
     */
    virtual IObjectProxyPtr GetSchemaProxy(EObjectType type) = 0;

    //! Creates a mutation that executes a request represented by #context.
    /*!
     *  Thread affinity: any
     */
    virtual std::unique_ptr<NHydra::TMutation> CreateExecuteMutation(
        const NYTree::IYPathServiceContextPtr& context,
        const NRpc::TAuthenticationIdentity& identity) = 0;

    //! Creates a mutation that destroys given objects.
    /*!
     *  Thread affinity: any
     */
    virtual std::unique_ptr<NHydra::TMutation> CreateDestroyObjectsMutation(
        const NProto::TReqDestroyObjects& request) = 0;

    //! Handles object destruction of both normal and sequoia objects.
    /*!
     *  Thread affinity: Automaton
     */
    virtual TFuture<void> DestroyObjects(std::vector<TObjectId> objectIds) = 0;

    //! Returns a future that gets set when the GC queues becomes empty.
    virtual TFuture<void> GCCollect() = 0;

    virtual TObject* CreateObject(
        TObjectId hintId,
        EObjectType type,
        NYTree::IAttributeDictionary* attributes) = 0;

    //! Returns true iff the object is in it's "active" life stage, i.e. it has
    //! been fully created and isn't being destroyed at the moment.
    virtual bool IsObjectLifeStageValid(const TObject* object) const = 0;

    //! Same as above, but throws if the object isn't in its "active" life stage.
    virtual void ValidateObjectLifeStage(const TObject* object) const = 0;

    struct TResolvePathOptions
    {
        bool EnablePartialResolve = false;
    };

    //! Handles paths to versioned and most unversioned objects.
    virtual TObject* ResolvePathToObject(
        const NYPath::TYPath& path,
        NTransactionServer::TTransaction* transaction,
        const TResolvePathOptions& options) = 0;

    struct TVersionedObjectPath
    {
        NYPath::TYPath Path;
        NTransactionServer::TTransactionId TransactionId;
    };

    virtual TFuture<std::vector<TErrorOr<TVersionedObjectPath>>> ResolveObjectIdsToPaths(
        const std::vector<TVersionedObjectId>& objectIds) = 0;

    //! Validates prerequisites, throws on failure.
    virtual void ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites) = 0;

    //! Forwards an object request to a given cell.
    virtual TFuture<TSharedRefArray> ForwardObjectRequest(
        const TSharedRefArray& requestMessage,
        TCellTag cellTag,
        NApi::EMasterChannelKind channelKind) = 0;

    //! Posts a creation request to the secondary master.
    virtual void ReplicateObjectCreationToSecondaryMaster(
        TObject* object,
        TCellTag cellTag) = 0;

    //! Posts an attribute update request to the secondary master.
    virtual void ReplicateObjectAttributesToSecondaryMaster(
        TObject* object,
        TCellTag cellTag) = 0;

    virtual NProfiling::TTimeCounter* GetMethodCumulativeExecuteTimeCounter(
        EObjectType type,
        const TString& method) = 0;

    virtual const TGarbageCollectorPtr& GetGarbageCollector() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IObjectManager)

////////////////////////////////////////////////////////////////////////////////

IObjectManagerPtr CreateObjectManager(NCellMaster::TBootstrap* bootstrap);
IObjectManagerPtr CreateObjectManager(TTestingTag, NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

