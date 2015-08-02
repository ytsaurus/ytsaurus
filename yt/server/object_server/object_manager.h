#pragma once

#include "public.h"
#include "type_handler.h"
#include "schema.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/profiling/profiler.h>

#include <ytlib/object_client/object_ypath_proxy.h>

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

//! Provides high-level management and tracking of objects.
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
    const IObjectTypeHandlerPtr& FindHandler(EObjectType type) const;

    //! Returns the handler for a given type.
    const IObjectTypeHandlerPtr& GetHandler(EObjectType type) const;

    //! Returns the handler for a given object.
    const IObjectTypeHandlerPtr& GetHandler(const TObjectBase* object) const;

    //! Returns the set of registered object types, excluding schemas.
    const std::set<EObjectType>& GetRegisteredTypes() const;

    //! If |hintId| is |NullObjectId| then creates a new unique object id.
    //! Otherwise returns |hintId| (but checks its type).
    TObjectId GenerateId(EObjectType type, const TObjectId& hintId);

    //! Returns |true| if the object was replicated here from another cell.
    bool IsForeign(const TObjectBase* object);

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

    //! Creates a cross-cell read-only proxy for the object with the given #id.
    NYTree::IYPathServicePtr CreateRemoteProxy(const TObjectId& id);

    //! Returns a proxy for the object with the given versioned id.
    IObjectProxyPtr GetProxy(
        TObjectBase* object,
        NTransactionServer::TTransaction* transaction = nullptr);

    //! Called when a versioned object is branched.
    void BranchAttributes(
        const TObjectBase* originatingObject,
        TObjectBase* branchedObject);

    //! Called when a versioned object is merged during transaction commit.
    void MergeAttributes(
        TObjectBase* originatingObject,
        const TObjectBase* branchedObject);

    //! Fills the attributes of a given unversioned object.
    void FillAttributes(
        TObjectBase* object,
        const NYTree::IAttributeDictionary& attributes);

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
        const TObjectId& hintId,
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        EObjectType type,
        NYTree::IAttributeDictionary* attributes,
        IObjectTypeHandler::TReqCreateObject* request,
        IObjectTypeHandler::TRspCreateObject* response);

    IObjectResolver* GetObjectResolver();

    //! Advices a client to yield if it spent a lot of time already.
    bool AdviceYield(NProfiling::TCpuInstant startInstant) const;

    //! Validates prerequisites, throws on failure.
    void ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites);

    //! Forwards a request to the leader of a given cell.
    TFuture<TSharedRefArray> ForwardToLeader(
        TCellTag cellTag,
        TSharedRefArray requestMessage,
        TNullable<TDuration> timeout = Null);

    //! Posts a creation request to the secondary master.
    void ReplicateObjectCreationToSecondaryMaster(
        TObjectBase* object,
        TCellTag cellTag);

    //! Posts an attribute update request to the secondary master.
    void ReplicateObjectAttributesToSecondaryMaster(
        TObjectBase* object,
        TCellTag cellTag);

    const NProfiling::TProfiler& GetProfiler();
    NProfiling::TTagId GetTypeTagId(EObjectType type);
    NProfiling::TTagId GetMethodTagId(const Stroka& method);

private:
    friend class TObjectProxyBase;

    class TRootService;
    typedef TIntrusivePtr<TRootService> TRootServicePtr;
    class TRemoteProxy;
    class TObjectResolver;

    const TObjectManagerConfigPtr Config_;

    NProfiling::TProfiler Profiler;

    struct TTypeEntry
    {
        IObjectTypeHandlerPtr Handler;
        TSchemaObject* SchemaObject = nullptr;
        IObjectProxyPtr SchemaProxy;
        NProfiling::TTagId TagId;
    };

    std::set<EObjectType> RegisteredTypes_;
    TEnumIndexedVector<TTypeEntry, EObjectType, MinObjectType, MaxObjectType> TypeToEntry_;

    yhash_map<Stroka, NProfiling::TTagId> MethodToTag_;

    TRootServicePtr RootService_;

    std::unique_ptr<TObjectResolver> ObjectResolver_;

    TObjectId MasterObjectId_;
    std::unique_ptr<TMasterObject> MasterObject_;

    IObjectProxyPtr MasterProxy_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    TGarbageCollectorPtr GarbageCollector_;

    int CreatedObjectCount_ = 0;
    int DestroyedObjectCount_ = 0;
    int LockedObjectCount_ = 0;

    //! Stores schemas (for serialization mostly).
    NHydra::TEntityMap<TObjectId, TSchemaObject> SchemaMap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(NCellMaster::TSaveContext& context) const;
    void SaveValues(NCellMaster::TSaveContext& context) const;

    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);

    virtual void OnRecoveryStarted() override;
    virtual void OnRecoveryComplete() override;
    virtual void Clear() override;
    virtual void OnLeaderActive() override;
    virtual void OnStopLeading() override;

    void HydraExecuteLeader(
        const NSecurityServer::TUserId& userId,
        const NRpc::TMutationId& mutationId,
        NRpc::IServiceContextPtr context);
    void HydraExecuteFollower(const NProto::TReqExecute& request);
    void HydraDestroyObjects(const NProto::TReqDestroyObjects& request);

    void OnProfiling();

    std::unique_ptr<NYTree::IAttributeDictionary> GetReplicatedAttributes(TObjectBase  * object);
    void OnSecondaryMasterRegistered(TCellTag cellTag);

};

DEFINE_REFCOUNTED_TYPE(TObjectManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

