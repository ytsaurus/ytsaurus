#pragma once

#include "public.h"

#include <yt/server/master/object_server/proto/object_manager.pb.h>

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/lib/hydra/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/ytlib/object_client/proto/object_ypath.pb.h>

#include <yt/core/profiling/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides high-level management and tracking of objects.
/*!
 *  \note
 *  Thread affinity: single-threaded
 */
class TObjectManager
    : public TRefCounted
{
public:
    explicit TObjectManager(NCellMaster::TBootstrap* bootstrap);

    ~TObjectManager();

    void Initialize();

    //! Registers a new type handler.
    void RegisterHandler(IObjectTypeHandlerPtr handler);

    //! Returns the handler for a given type or |nullptr| if the type is unknown.
    const IObjectTypeHandlerPtr& FindHandler(EObjectType type) const;

    //! Returns the handler for a given type.
    const IObjectTypeHandlerPtr& GetHandler(EObjectType type) const;

    //! Returns the handler for a given object.
    const IObjectTypeHandlerPtr& GetHandler(const TObject* object) const;

    //! Returns the set of registered object types, excluding schemas.
    const std::set<EObjectType>& GetRegisteredTypes() const;

    //! If |hintId| is |NullObjectId| then creates a new unique object id.
    //! Otherwise returns |hintId| (but checks its type).
    TObjectId GenerateId(EObjectType type, TObjectId hintId);

    //! Adds a reference.
    //! Returns the strong reference counter.
    int RefObject(TObject* object);

    //! Removes #count references.
    //! Returns the strong reference counter.
    int UnrefObject(TObject* object, int count = 1);

    //! Returns the current strong reference counter.
    int GetObjectRefCounter(TObject* object);

    //! Increments the object ephemeral reference counter thus temporarily preventing it from being destroyed.
    //! Returns the ephemeral reference counter.
    int EphemeralRefObject(TObject* object);

    //! Decrements the object ephemeral reference counter thus making it eligible for destruction.
    //! Returns the ephemeral reference counter.
    int EphemeralUnrefObject(TObject* object);

    //! Returns the current ephemeral reference counter.
    int GetObjectEphemeralRefCounter(TObject* object);

    //! Increments the object weak reference counter thus temporarily preventing it from being destroyed.
    //! Returns the weak reference counter.
    int WeakRefObject(TObject* object);

    //! Decrements the object weak reference counter thus making it eligible for destruction.
    //! Returns the weak reference counter.
    int WeakUnrefObject(TObject* object);

    //! Returns the current weak reference counter.
    int GetObjectWeakRefCounter(TObject* object);

    //! Finds object by id, returns |nullptr| if nothing is found.
    TObject* FindObject(TObjectId id);

    //! Finds object by type and attributes, returns |nullptr| if nothing is found.
    TObject* FindObjectByAttributes(EObjectType type, const NYTree::IAttributeDictionary* attributes);

    //! Finds object by id, fails if nothing is found.
    TObject* GetObject(TObjectId id);

    //! Finds object by id, throws if nothing is found.
    TObject* GetObjectOrThrow(TObjectId id);

    //! Find weak ghost object by id, fails if nothing is found.
    TObject* GetWeakGhostObject(TObjectId id);

    //! For object types requiring two-phase removal, initiates the removal protocol.
    //! For others, checks for the local reference counter and if it's 1, drops the last reference.
    void RemoveObject(TObject* object);

    //! Creates a cross-cell proxy for the object with the given #id.
    NYTree::IYPathServicePtr CreateRemoteProxy(TObjectId id);

    //! Creates a cross-cell proxy to forward the request to a given master cell.
    NYTree::IYPathServicePtr CreateRemoteProxy(TCellTag cellTag);

    //! Returns a proxy for the object with the given versioned id.
    IObjectProxyPtr GetProxy(
        TObject* object,
        NTransactionServer::TTransaction* transaction = nullptr);

    //! Called when a versioned object is branched.
    void BranchAttributes(
        const TObject* originatingObject,
        TObject* branchedObject);

    //! Called when a versioned object is merged during transaction commit.
    void MergeAttributes(
        TObject* originatingObject,
        const TObject* branchedObject);

    //! Fills the attributes of a given unversioned object.
    void FillAttributes(
        TObject* object,
        const NYTree::IAttributeDictionary& attributes);

    //! Returns a YPath service that routes all incoming requests.
    NYTree::IYPathServicePtr GetRootService();

    //! Returns "master" object for handling requests sent via TMasterYPathProxy.
    TObject* GetMasterObject();

    //! Returns a proxy for master object.
    /*!
     *  \see GetMasterObject
     */
    IObjectProxyPtr GetMasterProxy();

    //! Finds a schema object for a given type, returns |nullptr| if nothing is found.
    TObject* FindSchema(EObjectType type);

    //! Finds a schema object for a given type, fails if nothing is found.
    TObject* GetSchema(EObjectType type);

    //! Returns a proxy for schema object.
    /*!
     *  \see GetSchema
     */
    IObjectProxyPtr GetSchemaProxy(EObjectType type);

    //! Creates a mutation that executes a request represented by #context.
    /*!
     *  Thread affinity: any
     */
    std::unique_ptr<NHydra::TMutation> CreateExecuteMutation(
        const TString& userName,
        const NRpc::IServiceContextPtr& context);

    //! Creates a mutation that destroys given objects.
    /*!
     *  Thread affinity: any
     */
    std::unique_ptr<NHydra::TMutation> CreateDestroyObjectsMutation(
        const NProto::TReqDestroyObjects& request);

    //! Returns a future that gets set when the GC queues becomes empty.
    TFuture<void> GCCollect();

    TObject* CreateObject(
        TObjectId hintId,
        EObjectType type,
        NYTree::IAttributeDictionary* attributes);

    struct TResolvePathOptions
    {
        bool EnablePartialResolve = false;
        bool FollowPortals = true;
    };

    //! Handles paths to versioned and most unversioned objects.
    TObject* ResolvePathToObject(
        const NYPath::TYPath& path,
        NTransactionServer::TTransaction* transaction,
        const TResolvePathOptions& options);

    //! Validates prerequisites, throws on failure.
    void ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites);

    //! Forwards an object request to a given cell.
    TFuture<TSharedRefArray> ForwardObjectRequest(
        TSharedRefArray requestMessage,
        TCellTag cellTag,
        NHydra::EPeerKind peerKind);

    //! Posts a creation request to the secondary master.
    void ReplicateObjectCreationToSecondaryMaster(
        TObject* object,
        TCellTag cellTag);

    //! Posts a creation request to secondary masters.
    void ReplicateObjectCreationToSecondaryMasters(
        TObject* object,
        const TCellTagList& cellTags);

    //! Posts an attribute update request to the secondary master.
    void ReplicateObjectAttributesToSecondaryMaster(
        TObject* object,
        TCellTag cellTag);

    const NProfiling::TProfiler& GetProfiler();
    NProfiling::TMonotonicCounter* GetMethodCumulativeExecuteTimeCounter(EObjectType type, const TString& method);

    TEpoch GetCurrentEpoch();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TObjectManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

