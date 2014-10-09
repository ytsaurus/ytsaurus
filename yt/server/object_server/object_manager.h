#pragma once

#include "public.h"
#include "type_handler.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/profiling/profiler.h>

#include <server/hydra/mutation.h>
#include <server/hydra/entity_map.h>

#include <server/cell_master/automaton.h>

#include <server/transaction_server/public.h>

#include <server/object_server/object_manager.pb.h>

#include <server/security_server/public.h>

#include <server/cypress_server/public.h>

namespace NYT {
namespace NObjectServer {

// WinAPI is great.
#undef GetObject

////////////////////////////////////////////////////////////////////////////////

//! Similar to NYTree::INodeResolver but works for arbitrary objects rather than nodes.
struct IObjectResolver
{
    virtual ~IObjectResolver()
    { }

    //! Resolves a given path in the context of a given transaction.
    //! Throws if resolution fails.
    virtual IObjectProxyPtr ResolvePath(
        const NYPath::TYPath& path,
        NTransactionServer::TTransaction* transaction) = 0;

    //! Returns a path corresponding to a given object.
    virtual NYPath::TYPath GetPath(IObjectProxyPtr proxy) = 0;

};

////////////////////////////////////////////////////////////////////////////////

//! Provides high-level management and tracking of objects and their attributes.
/*!
 *  \note
 *  Thread affinity: single-threaded
 */
class TObjectManager
    : public NCellMaster::TMasterAutomatonPart
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

    //! Returns the list of registered object types, excluding schemas.
    const std::vector<EObjectType> GetRegisteredTypes() const;

    //! Creates a new unique object id.
    TObjectId GenerateId(EObjectType type);

    //! Adds a reference.
    void RefObject(TObjectBase* object);

    //! Removes a reference.
    void UnrefObject(TObjectBase* object);

    //! Increments the object weak reference counter thus temporarily preventing it from being destructed.
    void WeakRefObject(TObjectBase* object);

    //! Decrements the object weak reference counter thus making it eligible for destruction.
    void WeakUnrefObject(TObjectBase* object);

    //! Finds object by id, returns |nullptr| if nothing is found.
    TObjectBase* FindObject(const TObjectId& id);

    //! Finds object by id, fails if nothing is found.
    TObjectBase* GetObject(const TObjectId& id);

    //! Finds object by id, throws if nothing is found.
    TObjectBase* GetObjectOrThrow(const TObjectId& id);

    //! Returns a proxy for the object with the given versioned id.
    IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction = nullptr);

    //! Creates a new empty attribute set.
    TAttributeSet* CreateAttributes(const TVersionedObjectId& id);

    //! Creates a new empty attribute set if not exists.
    TAttributeSet* GetOrCreateAttributes(const TVersionedObjectId& id);

    //! Removes an existing attribute set.
    void RemoveAttributes(const TVersionedObjectId& id);

    //! Similar to #RemoveAttributes but may also be called for missing keys.
    //! Returns |true| if #id was found and removed.
    bool TryRemoveAttributes(const TVersionedObjectId& id);

    //! Called when a versioned object is branched.
    void BranchAttributes(
        const TVersionedObjectId& originatingId,
        const TVersionedObjectId& branchedId);

    //! Called when a versioned object is merged during transaction commit.
    void MergeAttributes(
        const TVersionedObjectId& originatingId,
        const TVersionedObjectId& branchedId);

    //! Returns a YPath service that routes all incoming requests.
    NYTree::IYPathServicePtr GetRootService();

    //! Returns "master" object for handling requests sent via TMasterYPathProxy.
    TObjectBase* GetMasterObject();

    //! Returns a proxy for master object.
    /*!
     *  \see GetMasterObject
     */
    IObjectProxyPtr GetMasterProxy();

    //! Finds a schema object for a given type, returns |nullptr| if nothing is found.
    TObjectBase* FindSchema(EObjectType type);

    //! Finds a schema object for a given type, fails if nothing is found.
    TObjectBase* GetSchema(EObjectType type);

    //! Returns a proxy for schema object.
    /*!
     *  \see GetSchema
     */
    IObjectProxyPtr GetSchemaProxy(EObjectType type);

    NHydra::TMutationPtr CreateExecuteMutation(
        const NProto::TReqExecute& request);

    NHydra::TMutationPtr CreateDestroyObjectsMutation(
        const NProto::TReqDestroyObjects& request);

    //! Returns a future that gets set when the GC queues becomes empty.
    TFuture<void> GCCollect();

    TObjectBase* CreateObject(
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        EObjectType type,
        NYTree::IAttributeDictionary* attributes,
        IObjectTypeHandler::TReqCreateObjects* request,
        IObjectTypeHandler::TRspCreateObjects* response);

    IObjectResolver* GetObjectResolver();

    void SuppressMutation();

    //! Advices a client to yield if it spent a lot of time already.
    bool AdviceYield(TInstant startTime) const;

    DECLARE_ENTITY_MAP_ACCESSORS(Attributes, TAttributeSet, TVersionedObjectId);

private:
    friend class TObjectProxyBase;

    class TRootService;
    typedef TIntrusivePtr<TRootService> TRootServicePtr;

    class TObjectResolver;

    TObjectManagerConfigPtr Config_;

    NProfiling::TProfiler Profiler;

    struct TTypeEntry
    {
        IObjectTypeHandlerPtr Handler;
        std::unique_ptr<TSchemaObject> SchemaObject;
        IObjectProxyPtr SchemaProxy;
        NProfiling::TTagId TagId;
    };

    std::vector<EObjectType> RegisteredTypes_;
    std::array<TTypeEntry, NObjectClient::MaxObjectType> TypeToEntry_;

    yhash_map<Stroka, NProfiling::TTagId> MethodToTag_;

    TRootServicePtr RootService_;

    std::unique_ptr<TObjectResolver> ObjectResolver_;

    TObjectId MasterObjectId_;
    std::unique_ptr<TMasterObject> MasterObject_;

    IObjectProxyPtr MasterProxy_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    TGarbageCollectorPtr GarbageCollector_;

    i64 CreatedObjectCount_ = 0;
    i64 DestroyedObjectCount_ = 0;
    int LockedObjectCount_ = 0;

    //! Stores deltas from parent transaction.
    NHydra::TEntityMap<TVersionedObjectId, TAttributeSet> AttributeMap_;

    bool MutationSuppressed_ = false;


    void SaveKeys(NCellMaster::TSaveContext& context) const;
    void SaveValues(NCellMaster::TSaveContext& context) const;
    void SaveSchemas(NCellMaster::TSaveContext& context) const;

    virtual void OnBeforeSnapshotLoaded() override;
    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);
    void LoadSchemas(NCellMaster::TLoadContext& context);

    virtual void OnRecoveryStarted() override;
    virtual void OnRecoveryComplete() override;

    void DoClear();
    virtual void Clear() override;

    virtual void OnLeaderActive() override;
    virtual void OnStopLeading() override;

    void InterceptProxyInvocation(
        TObjectProxyBase* proxy,
        NRpc::IServiceContextPtr context);
    void ExecuteMutatingRequest(
        const NSecurityServer::TUserId& userId,
        const NRpc::TMutationId& mutationId,
        NRpc::IServiceContextPtr context);

    void HydraExecute(const NProto::TReqExecute& request);
    void HydraDestroyObjects(const NProto::TReqDestroyObjects& request);

    NProfiling::TTagId GetTypeTagId(EObjectType type);
    NProfiling::TTagId GetMethodTagId(const Stroka& method);

    void OnProfiling();


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};

DEFINE_REFCOUNTED_TYPE(TObjectManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

