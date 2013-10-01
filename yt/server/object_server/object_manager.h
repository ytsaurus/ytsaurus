#pragma once

#include "public.h"
#include "type_handler.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/meta_state/mutation.h>

#include <core/profiling/profiler.h>

#include <server/cell_master/public.h>

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

    //! Returns the list of registered object types, excluding schemas.
    const std::vector<EObjectType> GetRegisteredTypes() const;

    //! Returns the cell id.
    TCellId GetCellId() const;

    //! Returns the cell unique id.
    const TGuid& GetCellGuid() const;

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

    TObjectBase* CreateObject(
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account,
        EObjectType type,
        NYTree::IAttributeDictionary* attributes,
        IObjectTypeHandler::TReqCreateObjects* request,
        IObjectTypeHandler::TRspCreateObjects* response);

    IObjectResolver* GetObjectResolver();

    DECLARE_METAMAP_ACCESSORS(Attributes, TAttributeSet, TVersionedObjectId);

private:
    typedef TObjectManager TThis;

    class TServiceContextWrapper;

    class TRootService;
    typedef TIntrusivePtr<TRootService> TRootServicePtr;

    class TObjectResolver;

    TObjectManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    NProfiling::TProfiler Profiler;

    struct TTypeEntry
    {
        IObjectTypeHandlerPtr Handler;
        std::unique_ptr<TSchemaObject> SchemaObject;
        IObjectProxyPtr SchemaProxy;
        NProfiling::TTagId TagId;
    };

    std::vector<EObjectType> RegisteredTypes;
    TTypeEntry TypeToEntry[NObjectClient::MaxObjectType];

    yhash_map<Stroka, NProfiling::TTagId> VerbToTag;

    TRootServicePtr RootService;

    std::unique_ptr<TObjectResolver> ObjectResolver;

    TObjectId MasterObjectId;
    std::unique_ptr<TMasterObject> MasterObject;

    IObjectProxyPtr MasterProxy;

    mutable TGuid CachedCellGuild;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor;

    TGarbageCollectorPtr GarbageCollector;

    i64 CreatedObjectCount;
    i64 DestroyedObjectCount;
    int LockedObjectCount;

    //! Stores deltas from parent transaction.
    NMetaState::TMetaStateMap<TVersionedObjectId, TAttributeSet> Attributes;

    void SaveKeys(NCellMaster::TSaveContext& context) const;
    void SaveValues(NCellMaster::TSaveContext& context) const;
    void SaveSchemas(NCellMaster::TSaveContext& context) const;

    virtual void OnBeforeLoaded() override;
    void LoadKeys(NCellMaster::TLoadContext& context);
    void LoadValues(NCellMaster::TLoadContext& context);
    void LoadSchemas(NCellMaster::TLoadContext& context);

    virtual void OnRecoveryStarted() override;
    virtual void OnRecoveryComplete() override;

    void DoClear();
    virtual void Clear() override;

    virtual void OnActiveQuorumEstablished() override;
    virtual void OnStopLeading() override;

    void ReplayVerb(const NProto::TMetaReqExecute& request);

    void DestroyObjects(const NProto::TMetaReqDestroyObjects& request);

    NProfiling::TTagId GetTypeTagId(EObjectType type);
    NProfiling::TTagId GetVerbTagId(const Stroka& verb);

    void OnProfiling();


    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

