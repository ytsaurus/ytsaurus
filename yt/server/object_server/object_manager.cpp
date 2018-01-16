#include "object_manager.h"
#include "private.h"
#include "config.h"
#include "garbage_collector.h"
#include "master.h"
#include "master_type_handler.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/chunk_server/chunk_list.h>

#include <yt/server/cypress_server/cypress_manager.h>
#include <yt/server/cypress_server/node_detail.h>

#include <yt/server/election/election_manager.h>

#include <yt/server/security_server/group.h>
#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>
#include <yt/server/security_server/account.h>

#include <yt/server/transaction_server/transaction.h>
#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/object_client/object_ypath_proxy.h>
#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/core/erasure/public.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/response_keeper.h>

#include <yt/core/ytree/node_detail.h>

#include <yt/core/ypath/tokenizer.h>

namespace NYT {
namespace NObjectServer {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NHydra;
using namespace NRpc;
using namespace NBus;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NChunkServer;
using namespace NObjectClient;
using namespace NHydra;
using namespace NCellMaster;
using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ObjectServerLogger;
static const auto ProfilingPeriod = TDuration::MilliSeconds(100);
static const IObjectTypeHandlerPtr NullTypeHandler;

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRemoteProxy
    : public IYPathService
{
public:
    TRemoteProxy(TBootstrap* bootstrap, const TObjectId& objectId)
        : Bootstrap_(bootstrap)
        , ObjectId_(objectId)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& context) override
    {
        const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        if (ypathExt.mutating()) {
            THROW_ERROR_EXCEPTION("Mutating requests to remote cells are not allowed");
        }

        YCHECK(!HasMutationContext());

        return TResolveResultHere{path};
    }

    virtual void Invoke(const IServiceContextPtr& context) override
    {
        auto requestMessage = context->GetRequestMessage();
        auto requestHeader = context->RequestHeader();

        auto updatedYPath = FromObjectId(ObjectId_) + GetRequestYPath(requestHeader);
        SetRequestYPath(&requestHeader, updatedYPath);
        auto updatedMessage = SetRequestHeader(requestMessage, requestHeader);

        auto cellTag = CellTagFromId(ObjectId_);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto asyncResponseMessage = objectManager->ForwardToLeader(cellTag, updatedMessage);
        context->ReplyFrom(std::move(asyncResponseMessage));
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TNullable<std::vector<TString>>& /*attributeKeys*/,
        bool /*stable*/) override
    {
        Y_UNREACHABLE();
    }

    virtual bool ShouldHideAttributes() override
    {
        return false;
    }

private:
    TBootstrap* const Bootstrap_;
    const TObjectId ObjectId_;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TRootService
    : public IYPathService
{
public:
    explicit TRootService(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& context) override
    {
        const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        if (ypathExt.mutating() && !HasMutationContext()) {
            // Nested call or recovery.
            return DoResolveHere(path);
        } else {
            // Read-only request.
            return DoResolveThere(path, context);
        }
    }

    virtual void Invoke(const IServiceContextPtr& context) override
    {
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        if (hydraManager->IsFollower()) {
            ForwardToLeader(context);
            return;
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager
            ->CreateExecuteMutation(user->GetName(), context)
            ->Commit()
            .Subscribe(BIND([=] (const TErrorOr<TMutationResponse>& result) {
                if (!result.IsOK()) {
                    // Reply with commit error.
                    context->Reply(result);
                }
            }));
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* /*consumer*/,
        const TNullable<std::vector<TString>>& /*attributeKeys*/,
        bool /*stable*/) override
    {
        Y_UNREACHABLE();
    }

    virtual bool ShouldHideAttributes() override
    {
        Y_UNREACHABLE();
    }

private:
    TBootstrap* const Bootstrap_;


    static TResolveResult DoResolveHere(const TYPath& path)
    {
        return TResolveResultHere{path};
    }

    TResolveResult DoResolveThere(const TYPath& path, const IServiceContextPtr& context)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        if (context->GetService() == TMasterYPathProxy::GetDescriptor().ServiceName) {
            return TResolveResultThere{objectManager->GetMasterProxy(), TYPath()};
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto transactionId = GetTransactionId(context);
        auto* transaction = transactionId
            ? transactionManager->GetTransactionOrThrow(transactionId)
            : nullptr;

        NYPath::TTokenizer tokenizer(path);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::EndOfStream:
                THROW_ERROR_EXCEPTION("YPath cannot be empty");

            case NYPath::ETokenType::Slash: {
                auto root = cypressManager->GetNodeProxy(
                    cypressManager->GetRootNode(),
                    transaction);
                return TResolveResultThere{std::move(root), TYPath(tokenizer.GetSuffix())};
            }

            case NYPath::ETokenType::Literal: {
                const auto& token = tokenizer.GetToken();
                if (!token.StartsWith(ObjectIdPathPrefix)) {
                    tokenizer.ThrowUnexpected();
                }

                TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
                TObjectId objectId;
                if (!TObjectId::FromString(objectIdString, &objectId)) {
                    THROW_ERROR_EXCEPTION("Error parsing object id %v",
                        objectIdString);
                }

                tokenizer.Advance();

                bool foreign =
                    CellTagFromId(objectId) != Bootstrap_->GetCellTag() &&
                    Bootstrap_->IsPrimaryMaster();
                
                bool suppressRedirect = false;
                if (foreign && tokenizer.GetType() == NYPath::ETokenType::Ampersand) {
                    suppressRedirect = true;
                    tokenizer.Advance();
                }

                IYPathServicePtr proxy;
                if (foreign && !suppressRedirect) {
                    proxy = objectManager->CreateRemoteProxy(objectId);
                } else {
                    auto* object = (context->GetMethod() == "Exists")
                        ? objectManager->FindObject(objectId)
                        : objectManager->GetObjectOrThrow(objectId);
                    proxy = IsObjectAlive(object)
                        ? objectManager->GetProxy(object, transaction)
                        : TNonexistingService::Get();
                }
                return TResolveResultThere{std::move(proxy), TYPath(tokenizer.GetInput())};
            }

            default:
                tokenizer.ThrowUnexpected();
                Y_UNREACHABLE();
        }
    }


    void ForwardToLeader(const IServiceContextPtr& context)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto asyncResponseMessage = objectManager->ForwardToLeader(
            Bootstrap_->GetCellTag(),
            context->GetRequestMessage());
        context->ReplyFrom(std::move(asyncResponseMessage));
    }

};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(
    TObjectManagerConfigPtr config,
    TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap)
    , Config_(config)
    , Profiler(ObjectServerProfiler)
    , RootService_(New<TRootService>(Bootstrap_))
    , GarbageCollector_(New<TGarbageCollector>(Config_, Bootstrap_))
{
    YCHECK(config);
    YCHECK(bootstrap);

    RegisterLoader(
        "ObjectManager.Keys",
        BIND(&TObjectManager::LoadKeys, Unretained(this)));
    RegisterLoader(
        "ObjectManager.Values",
        BIND(&TObjectManager::LoadValues, Unretained(this)));

    RegisterSaver(
        ESyncSerializationPriority::Keys,
        "ObjectManager.Keys",
        BIND(&TObjectManager::SaveKeys, Unretained(this)));
    RegisterSaver(
        ESyncSerializationPriority::Values,
        "ObjectManager.Values",
        BIND(&TObjectManager::SaveValues, Unretained(this)));

    RegisterHandler(CreateMasterTypeHandler(Bootstrap_));

    RegisterMethod(BIND(&TObjectManager::HydraExecuteFollower, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraDestroyObjects, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraCreateForeignObject, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraRemoveForeignObject, Unretained(this)));
    RegisterMethod(BIND(&TObjectManager::HydraUnrefExportedObjects, Unretained(this)));

    MasterObjectId_ = MakeWellKnownId(EObjectType::Master, Bootstrap_->GetPrimaryCellTag());
}

void TObjectManager::Initialize()
{
    if (Bootstrap_->IsPrimaryMaster()) {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->SubscribeReplicateValuesToSecondaryMaster(
            BIND(&TObjectManager::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
    }

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(),
        BIND(&TObjectManager::OnProfiling, MakeWeak(this)),
        ProfilingPeriod);
    ProfilingExecutor_->Start();
}

IYPathServicePtr TObjectManager::GetRootService()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RootService_;
}

TObjectBase* TObjectManager::GetMasterObject()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterObject_.get();
}

IObjectProxyPtr TObjectManager::GetMasterProxy()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MasterProxy_;
}

TObjectBase* TObjectManager::FindSchema(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return TypeToEntry_[type].SchemaObject;
}

TObjectBase* TObjectManager::GetSchema(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* schema = FindSchema(type);
    YCHECK(schema);
    return schema;
}

IObjectProxyPtr TObjectManager::GetSchemaProxy(EObjectType type)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& entry = TypeToEntry_[type];
    YCHECK(entry.SchemaProxy);
    return entry.SchemaProxy;
}

void TObjectManager::RegisterHandler(IObjectTypeHandlerPtr handler)
{
    // No thread affinity check here.
    // This will be called during init-time only but from an unspecified thread.
    YCHECK(handler);

    auto type = handler->GetType();
    YCHECK(!TypeToEntry_[type].Handler);
    YCHECK(RegisteredTypes_.insert(type).second);
    auto& entry = TypeToEntry_[type];
    entry.Handler = handler;

    if (HasSchema(type)) {
        auto schemaType = SchemaTypeFromType(type);
        TypeToEntry_[schemaType].Handler = CreateSchemaTypeHandler(Bootstrap_, type);

        auto schemaObjectId = MakeSchemaObjectId(type, Bootstrap_->GetPrimaryCellTag());

        LOG_INFO("Type registered (Type: %v, SchemaObjectId: %v)",
            type,
            schemaObjectId);
    } else {
        LOG_INFO("Type registered (Type: %v)",
            type);
    }
}

const IObjectTypeHandlerPtr& TObjectManager::FindHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return type >= MinObjectType && type <= MaxObjectType
        ? TypeToEntry_[type].Handler
        : NullTypeHandler;
}

const IObjectTypeHandlerPtr& TObjectManager::GetHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& handler = FindHandler(type);
    Y_ASSERT(handler);
    return handler;
}

const IObjectTypeHandlerPtr& TObjectManager::GetHandler(const TObjectBase* object) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return GetHandler(object->GetType());
}

const std::set<EObjectType>& TObjectManager::GetRegisteredTypes() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return RegisteredTypes_;
}

TObjectId TObjectManager::GenerateId(EObjectType type, const TObjectId& hintId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* mutationContext = GetCurrentMutationContext();
    auto version = mutationContext->GetVersion();
    auto hash = mutationContext->RandomGenerator().Generate<ui32>();

    auto cellTag = Bootstrap_->GetCellTag();

    auto id = hintId
        ? hintId
        : MakeRegularId(type, cellTag, version, hash);
    Y_ASSERT(TypeFromId(id) == type);

    ++CreatedObjects_;

    return id;
}

int TObjectManager::RefObject(TObjectBase* object)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(!object->IsDestroyed());
    Y_ASSERT(object->IsTrunk());

    int refCounter = object->RefObject();
    LOG_TRACE_UNLESS(IsRecovery(), "Object referenced (Id: %v, RefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        GetObjectWeakRefCounter(object));

    if (refCounter == 1) {
        GarbageCollector_->UnregisterZombie(object);
    }

    return refCounter;
}

int TObjectManager::UnrefObject(TObjectBase* object, int count)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(object->IsAlive());
    Y_ASSERT(object->IsTrunk());

    int refCounter = object->UnrefObject(count);
    LOG_TRACE_UNLESS(IsRecovery(), "Object unreferenced (Id: %v, RefCounter: %v, WeakRefCounter: %v)",
        object->GetId(),
        refCounter,
        GetObjectWeakRefCounter(object));

    if (refCounter == 0) {
        const auto& handler = GetHandler(object);
        handler->ZombifyObject(object);

        GarbageCollector_->RegisterZombie(object);

        if (Bootstrap_->IsPrimaryMaster()) {
            auto flags = handler->GetFlags();
            if (Any(flags & ETypeFlags::ReplicateDestroy)) {
                NProto::TReqRemoveForeignObject request;
                ToProto(request.mutable_object_id(), object->GetId());

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                auto replicationCellTags = handler->GetReplicationCellTags(object);
                multicellManager->PostToMasters(request, replicationCellTags);
            }
        }
    }
    return refCounter;
}

int TObjectManager::GetObjectRefCounter(TObjectBase* object)
{
    return object->GetObjectRefCounter();
}

int TObjectManager::WeakRefObject(TObjectBase* object)
{
    return GarbageCollector_->WeakRefObject(object, CurrentEpoch_);
}

int TObjectManager::WeakUnrefObject(TObjectBase* object)
{
    return GarbageCollector_->WeakUnrefObject(object, CurrentEpoch_);
}

int TObjectManager::GetObjectWeakRefCounter(TObjectBase* object)
{
    return object->GetObjectWeakRefCounter(CurrentEpoch_);
}

void TObjectManager::SaveKeys(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveKeys(context);
}

void TObjectManager::SaveValues(NCellMaster::TSaveContext& context) const
{
    SchemaMap_.SaveValues(context);
    GarbageCollector_->Save(context);
}

void TObjectManager::LoadKeys(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SchemaMap_.LoadKeys(context);
}

void TObjectManager::LoadValues(NCellMaster::TLoadContext& context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SchemaMap_.LoadValues(context);

    // COMPAT(sandello): CellNodeMap (408) and CellNode (410) are now obsolete.
    // COMPAT(babenko): unfortunately, 408 and 410 are _in use_ again in 19.* :(
    if (context.GetVersion() < 400) {
        for (auto type : {408, 410}) {
            auto id = MakeSchemaObjectId(EObjectType(type), Bootstrap_->GetPrimaryCellTag());
            SchemaMap_.TryRemove(id);
        }
    }

    InitSchemas();

    GarbageCollector_->Load(context);
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::Clear();

    MasterObject_.reset(new TMasterObject(MasterObjectId_));
    MasterObject_->RefObject();

    MasterProxy_ = GetProxy(MasterObject_.get());

    SchemaMap_.Clear();

    InitSchemas();

    CreatedObjects_ = 0;
    DestroyedObjects_ = 0;

    GarbageCollector_->Clear();
}

void TObjectManager::InitSchemas()
{
    for (auto& entry : TypeToEntry_) {
        entry.SchemaObject = nullptr;
        entry.SchemaProxy.Reset();
    }

    for (auto type : RegisteredTypes_) {
        if (!HasSchema(type)) {
            continue;
        }

        auto id = MakeSchemaObjectId(type, Bootstrap_->GetPrimaryCellTag());
        if (!SchemaMap_.Contains(id)) {
            auto schemaObject = std::make_unique<TSchemaObject>(id);
            schemaObject->RefObject();
            SchemaMap_.Insert(id, std::move(schemaObject));
        }

        auto& entry = TypeToEntry_[type];
        entry.SchemaObject = SchemaMap_.Get(id);
        entry.SchemaProxy = GetProxy(entry.SchemaObject);
    }
}

void TObjectManager::OnRecoveryStarted()
{
    Profiler.SetEnabled(false);

    ++CurrentEpoch_;
    GarbageCollector_->Reset();
}

void TObjectManager::OnRecoveryComplete()
{
    Profiler.SetEnabled(true);
}

void TObjectManager::OnLeaderActive()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    GarbageCollector_->Start();
}

void TObjectManager::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    GarbageCollector_->Stop();
}

TObjectBase* TObjectManager::FindObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->FindObject(id);
}

TObjectBase* TObjectManager::GetObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* object = FindObject(id);
    YCHECK(object);
    return object;
}

TObjectBase* TObjectManager::GetObjectOrThrow(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* object = FindObject(id);
    if (!IsObjectAlive(object)) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "No such object %v",
            id);
    }

    return object;
}

IYPathServicePtr TObjectManager::CreateRemoteProxy(const TObjectId& id)
{
    return New<TRemoteProxy>(Bootstrap_, id);
}

IObjectProxyPtr TObjectManager::GetProxy(
    TObjectBase* object,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(IsObjectAlive(object));

    const auto& id = object->GetId();
    const auto& handler = FindHandler(TypeFromId(id));
    if (!handler) {
        return nullptr;
    }

    return handler->GetProxy(object, transaction);
}

void TObjectManager::BranchAttributes(
    const TObjectBase* /*originatingObject*/,
    TObjectBase* /*branchedObject*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    // We don't store empty deltas at the moment
}

void TObjectManager::MergeAttributes(
    TObjectBase* originatingObject,
    const TObjectBase* branchedObject)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto* branchedAttributes = branchedObject->GetAttributes();
    if (!branchedAttributes)
        return;

    auto* originatingAttributes = originatingObject->GetMutableAttributes();
    for (const auto& pair : branchedAttributes->Attributes()) {
        if (!pair.second && originatingObject->IsTrunk()) {
            originatingAttributes->Attributes().erase(pair.first);
        } else {
            originatingAttributes->Attributes()[pair.first] = pair.second;
        }
    }

    if (originatingAttributes->Attributes().empty()) {
        originatingObject->ClearAttributes();
    }
}

void TObjectManager::FillAttributes(
    TObjectBase* object,
    const IAttributeDictionary& attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto keys = attributes.List();
    if (keys.empty()) {
        return;
    }

    auto proxy = GetProxy(object, nullptr);
    std::sort(keys.begin(), keys.end());
    for (const auto& key : keys) {
        auto value = attributes.GetYson(key);
        proxy->MutableAttributes()->Set(key, value);
    }
}

std::unique_ptr<TMutation> TObjectManager::CreateExecuteMutation(
    const TString& userName,
    const IServiceContextPtr& context)
{
    NProto::TReqExecute request;
    request.set_user_name(userName);
    auto requestMessage = context->GetRequestMessage();
    for (const auto& part : requestMessage) {
        request.add_request_parts(part.Begin(), part.Size());
    }

    auto mutation = CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request);
    mutation->SetHandler(BIND(
        &TObjectManager::HydraExecuteLeader,
        MakeStrong(this),
        userName,
        context));
    return mutation;
}

std::unique_ptr<TMutation> TObjectManager::CreateDestroyObjectsMutation(const NProto::TReqDestroyObjects& request)
{
    return CreateMutation(
        Bootstrap_->GetHydraFacade()->GetHydraManager(),
        request,
        &TObjectManager::HydraDestroyObjects,
        this);
}

TFuture<void> TObjectManager::GCCollect()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return GarbageCollector_->Collect();
}

TObjectBase* TObjectManager::CreateObject(
    const TObjectId& hintId,
    EObjectType type,
    IAttributeDictionary* attributes)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& handler = FindHandler(type);
    if (!handler) {
        THROW_ERROR_EXCEPTION("Unknown object type %v",
            type);
    }

    auto flags = handler->GetFlags();
    if (None(flags & ETypeFlags::Creatable)) {
        THROW_ERROR_EXCEPTION("Objects of type %Qlv cannot be created explicitly",
            type);
    }
    
    bool replicate =
        Bootstrap_->IsPrimaryMaster() &&
        Any(flags & ETypeFlags::ReplicateCreate);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    auto* schema = FindSchema(type);
    if (schema) {
        securityManager->ValidatePermission(schema, user, EPermission::Create);
    }

    std::unique_ptr<IAttributeDictionary> attributeHolder;
    if (auto* attributeSet = schema->GetAttributes()) {
        attributeHolder = CreateEphemeralAttributes();
        for (const auto& pair : attributeSet->Attributes()) {
            attributeHolder->SetYson(pair.first, pair.second);
        }
        if (attributes) {
            auto attributeMap = PatchNode(attributeHolder->ToMap(), attributes->ToMap());
            attributeHolder = IAttributeDictionary::FromMap(attributeMap->AsMap());
        }
        attributes = attributeHolder.get();
    } else if (!attributes) {
        attributeHolder = CreateEphemeralAttributes();
        attributes = attributeHolder.get();
    }

    // ITypeHandler::CreateObject may modify the attributes.
    std::unique_ptr<IAttributeDictionary> replicatedAttributes;
    if (replicate) {
        replicatedAttributes = attributes->Clone();
    }

    auto* object = handler->CreateObject(hintId, attributes);

    YCHECK(object->GetObjectRefCounter() == 1);

    if (CellTagFromId(object->GetId()) != Bootstrap_->GetCellTag()) {
        object->SetForeign();
    }

    try {
        FillAttributes(object, *attributes);
    } catch (const std::exception& ex) {
        UnrefObject(object);
        throw;
    }
 
    auto* acd = securityManager->FindAcd(object);
    if (acd) {
        acd->SetOwner(user);
    }

    if (replicate) {
        NProto::TReqCreateForeignObject replicationRequest;
        ToProto(replicationRequest.mutable_object_id(), object->GetId());
        replicationRequest.set_type(static_cast<int>(type));
        ToProto(replicationRequest.mutable_object_attributes(), *replicatedAttributes);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto replicationCellTags = handler->GetReplicationCellTags(object);
        multicellManager->PostToMasters(replicationRequest, replicationCellTags);
    }

    return object;
}

TObjectBase* TObjectManager::ResolvePathToObject(const TYPath& path, TTransaction* transaction)
{
    // Shortcut.
    if (path.empty()) {
        return GetMasterObject();
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();

    NYPath::TTokenizer tokenizer(path);

    auto doResolve = [&] (IObjectProxy* proxy) {
        // Shortcut.
        auto suffixPath = tokenizer.GetSuffix();
        if (suffixPath.empty()) {
            return proxy->GetObject();
        }

        // Slow path.
        auto req = TObjectYPathProxy::GetBasicAttributes(TYPath(suffixPath));
        SetTransactionId(req, GetObjectId(transaction));
        auto rsp = SyncExecuteVerb(proxy, req);
        auto objectId = FromProto<TObjectId>(rsp->object_id());
        return GetObjectOrThrow(objectId);
    };

    switch (tokenizer.Advance()) {
        case NYPath::ETokenType::EndOfStream:
            return GetMasterObject();

        case NYPath::ETokenType::Slash: {
            auto root = cypressManager->GetNodeProxy(
                cypressManager->GetRootNode(),
                transaction);
            return doResolve(root.Get());
        }

        case NYPath::ETokenType::Literal: {
            const auto& token = tokenizer.GetToken();
            if (!token.StartsWith(ObjectIdPathPrefix)) {
                tokenizer.ThrowUnexpected();
            }

            TStringBuf objectIdString(token.begin() + ObjectIdPathPrefix.length(), token.end());
            TObjectId objectId;
            if (!TObjectId::FromString(objectIdString, &objectId)) {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::ResolveError,
                    "Error parsing object id %Qv",
                    objectIdString);
            }

            auto* object = objectManager->GetObjectOrThrow(objectId);
            auto proxy = objectManager->GetProxy(object, transaction);
            return doResolve(proxy.Get());
        }

        default:
            tokenizer.ThrowUnexpected();
            Y_UNREACHABLE();
    }
}

void TObjectManager::ValidatePrerequisites(const NObjectClient::NProto::TPrerequisitesExt& prerequisites)
{
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    const auto& cypressManager = Bootstrap_->GetCypressManager();

    auto getPrerequisiteTransaction = [&] (const TTransactionId& transactionId) {
        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!IsObjectAlive(transaction)) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is missing",
                transactionId);
        }
        if (transaction->GetPersistentState() != ETransactionState::Active) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: transaction %v is not active",
                transactionId);
        }
        return transaction;
    };

    for (const auto& prerequisite : prerequisites.transactions()) {
        auto transactionId = FromProto<TTransactionId>(prerequisite.transaction_id());
        getPrerequisiteTransaction(transactionId);
    }

    for (const auto& prerequisite : prerequisites.revisions()) {
        auto transactionId = FromProto<TTransactionId>(prerequisite.transaction_id());
        const auto& path = prerequisite.path();
        i64 revision = prerequisite.revision();

        auto* transaction = transactionId
            ? getPrerequisiteTransaction(transactionId)
            : nullptr;

        TCypressNodeBase* trunkNode;
        try {
            trunkNode = cypressManager->ResolvePathToTrunkNode(path, transaction);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: failed to resolve path %v",
                path)
                << ex;
        }

        if (trunkNode->GetRevision() != revision) {
            THROW_ERROR_EXCEPTION(
                NObjectClient::EErrorCode::PrerequisiteCheckFailed,
                "Prerequisite check failed: node %v revision mismatch: expected %v, found %v",
                path,
                revision,
                trunkNode->GetRevision());
        }
    }
}

TFuture<TSharedRefArray> TObjectManager::ForwardToLeader(
    TCellTag cellTag,
    TSharedRefArray requestMessage,
    TNullable<TDuration> timeout)
{
    NRpc::NProto::TRequestHeader header;
    YCHECK(ParseRequestHeader(requestMessage, &header));

    auto requestId = FromProto<TRequestId>(header.request_id());
    const auto& ypathExt = header.GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);

    LOG_DEBUG("Forwarding request to leader (RequestId: %v, Invocation: %v:%v %v, CellTag: %v, Timeout: %v)",
        requestId,
        header.service(),
        header.method(),
        ypathExt.path(),
        cellTag,
        timeout);

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto* user = securityManager->GetAuthenticatedUser();

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    auto channel = multicellManager->GetMasterChannelOrThrow(
        cellTag,
        EPeerKind::Leader);

    TObjectServiceProxy proxy(std::move(channel));
    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetTimeout(timeout);
    batchReq->SetUser(user->GetName());
    batchReq->AddRequestMessage(requestMessage);

    return batchReq->Invoke().Apply(BIND([=] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Request forwarding failed");

        LOG_DEBUG("Request forwarding succeeded (RequestId: %v)",
            requestId);

        const auto& batchRsp = batchRspOrError.Value();
        return batchRsp->GetResponseMessage(0);
    }));
}

void TObjectManager::ReplicateObjectCreationToSecondaryMaster(
    TObjectBase* object,
    TCellTag cellTag)
{
    ReplicateObjectCreationToSecondaryMasters(object, {cellTag});
}

void TObjectManager::ReplicateObjectCreationToSecondaryMasters(
    TObjectBase* object,
    const TCellTagList& cellTags)
{
    if (cellTags.empty()) {
        return;
    }

    if (object->IsBuiltin()) {
        return;
    }

    NProto::TReqCreateForeignObject request;
    ToProto(request.mutable_object_id(), object->GetId());
    request.set_type(static_cast<int>(object->GetType()));
    ToProto(request.mutable_object_attributes(), *GetReplicatedAttributes(object, true));

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMasters(request, cellTags);
}

void TObjectManager::ReplicateObjectAttributesToSecondaryMaster(
    TObjectBase* object,
    TCellTag cellTag)
{
    auto req = TYPathProxy::Set(FromObjectId(object->GetId()) + "/@");
    req->set_value(ConvertToYsonString(GetReplicatedAttributes(object, false)->ToMap()).GetData());

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    multicellManager->PostToMaster(req, cellTag);
}

void TObjectManager::HydraExecuteLeader(
    const TString& userName,
    const IServiceContextPtr& context,
    TMutationContext*)
{
    TWallTimer timer;

    const auto& securityManager = Bootstrap_->GetSecurityManager();

    TUser* user = nullptr;
    try {
        user = securityManager->GetUserByNameOrThrow(userName);
        TAuthenticatedUserGuard userGuard(securityManager, user);
        ExecuteVerb(RootService_, context);
    } catch (const std::exception& ex) {
        context->Reply(ex);
    }

    if (!IsRecovery() && IsObjectAlive(user)) {
        securityManager->ChargeUserWrite(user, 1, timer.GetElapsedTime());
    }

    auto mutationId = context->GetMutationId();
    if (mutationId) {
        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& responseKeeper = hydraFacade->GetResponseKeeper();
        // NB: Context must already be replied by now.
        responseKeeper->EndRequest(mutationId, context->GetResponseMessage());
    }
}

void TObjectManager::HydraExecuteFollower(NProto::TReqExecute* request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& userName = request->user_name();

    std::vector<TSharedRef> parts(request->request_parts_size());
    for (int partIndex = 0; partIndex < request->request_parts_size(); ++partIndex) {
        parts[partIndex] = TSharedRef::FromString(request->request_parts(partIndex));
    }

    auto requestMessage = TSharedRefArray(std::move(parts));
    auto context = CreateYPathContext(std::move(requestMessage));
    HydraExecuteLeader(userName, std::move(context), nullptr);
}

void TObjectManager::HydraDestroyObjects(NProto::TReqDestroyObjects* request)
{
    // NB: Ordered map is a must to make the behavior deterministic.
    std::map<TCellTag, NProto::TReqUnrefExportedObjects> crossCellRequestMap;
    auto getCrossCellRequest = [&] (const TObjectId& id) -> NProto::TReqUnrefExportedObjects& {
        return crossCellRequestMap[CellTagFromId(id)];
    };

    for (const auto& protoId : request->object_ids()) {
        auto id = FromProto<TObjectId>(protoId);
        auto type = TypeFromId(id);

        const auto& handler = GetHandler(type);
        auto* object = handler->FindObject(id);

        if (!object || object->GetObjectRefCounter() > 0)
            continue;

        if (object->IsForeign() && object->GetImportRefCounter() > 0) {
            auto& crossCellRequest = getCrossCellRequest(id);
            crossCellRequest.set_cell_tag(Bootstrap_->GetCellTag());
            auto* entry = crossCellRequest.add_entries();
            ToProto(entry->mutable_object_id(), id);
            entry->set_import_ref_counter(object->GetImportRefCounter());
        }

        // NB: The order of Dequeue/Destroy/CheckEmpty calls matters.
        // CheckEmpty will raise CollectPromise_ when GC queue becomes empty.
        // To enable cascaded GC sweep we don't want this to happen
        // if some ids are added during DestroyObject.
        GarbageCollector_->DestroyZombie(object);
        ++DestroyedObjects_;

        LOG_DEBUG_UNLESS(IsRecovery(), "Object destroyed (Type: %v, Id: %v)",
            type,
            id);
    }

    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    for (const auto& pair : crossCellRequestMap) {
        auto cellTag = pair.first;
        const auto& perCellRequest = pair.second;
        multicellManager->PostToMaster(perCellRequest, cellTag);
        LOG_DEBUG_UNLESS(IsRecovery(), "Requesting to unreference imported objects (CellTag: %v, Count: %v)",
            cellTag,
            perCellRequest.entries_size());
    }

    GarbageCollector_->CheckEmpty();
}

void TObjectManager::HydraCreateForeignObject(NProto::TReqCreateForeignObject* request) noexcept
{
    auto objectId = FromProto<TObjectId>(request->object_id());
    auto type = EObjectType(request->type());

    auto attributes = request->has_object_attributes()
        ? FromProto(request->object_attributes())
        : std::unique_ptr<IAttributeDictionary>();

    CreateObject(
        objectId,
        type,
        attributes.get());

    LOG_DEBUG_UNLESS(IsRecovery(), "Foreign object created (Id: %v, Type: %v)",
        objectId,
        type);
}

void TObjectManager::HydraRemoveForeignObject(NProto::TReqRemoveForeignObject* request) noexcept
{
    auto objectId = FromProto<TObjectId>(request->object_id());

    auto* object = FindObject(objectId);
    if (object) {
        LOG_DEBUG_UNLESS(IsRecovery(), "Removing foreign object (ObjectId: %v, RefCounter: %v)",
            objectId,
            object->GetObjectRefCounter());
        UnrefObject(object);
    } else {
        LOG_DEBUG_UNLESS(IsRecovery(), "Attempt to remove a non-existing foreign object (ObjectId: %v)",
            objectId);
    }
}

void TObjectManager::HydraUnrefExportedObjects(NProto::TReqUnrefExportedObjects* request) noexcept
{
    auto cellTag = request->cell_tag();

    for (const auto& entry : request->entries()) {
        auto objectId = FromProto<TObjectId>(entry.object_id());
        auto importRefCounter = entry.import_ref_counter();

        auto* object = GetObject(objectId);
        UnrefObject(object, importRefCounter);

        const auto& handler = GetHandler(object);
        handler->UnexportObject(object, cellTag, importRefCounter);
    }

    LOG_DEBUG_UNLESS(IsRecovery(), "Exported objects unreferenced (CellTag: %v, Count: %v)",
        cellTag,
        request->entries_size());
}

const TProfiler& TObjectManager::GetProfiler()
{
    return Profiler;
}

NProfiling::TAggregateCounter* TObjectManager::GetMethodExecTimeCounter(EObjectType type, const TString& method)
{
    auto key = std::make_pair(type, method);
    auto it = MethodToEntry_.find(key);
    if (it == MethodToEntry_.end()) {
        auto entry = std::make_unique<TMethodEntry>();
        entry->ExecTimeCounter = NProfiling::TAggregateCounter(
            "/verb_execute_time",
            {
                TProfileManager::Get()->RegisterTag("type", type),
                TProfileManager::Get()->RegisterTag("method", method)
            });
        it = MethodToEntry_.emplace(key, std::move(entry)).first;
    }
    return &it->second->ExecTimeCounter;
}

TEpoch TObjectManager::GetCurrentEpoch()
{
    return CurrentEpoch_;
}

void TObjectManager::OnProfiling()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    Profiler.Enqueue("/zombie_object_count", GarbageCollector_->GetZombieCount(), EMetricType::Gauge);
    Profiler.Enqueue("/ghost_object_count", GarbageCollector_->GetGhostCount(), EMetricType::Gauge);
    Profiler.Enqueue("/locked_object_count", GarbageCollector_->GetLockedCount(), EMetricType::Gauge);
    Profiler.Enqueue("/created_objects", CreatedObjects_, EMetricType::Counter);
    Profiler.Enqueue("/destroyed_objects", DestroyedObjects_, EMetricType::Counter);
}

std::unique_ptr<NYTree::IAttributeDictionary> TObjectManager::GetReplicatedAttributes(
    TObjectBase* object,
    bool mandatory)
{
    YCHECK(!IsVersionedType(object->GetType()));

    const auto& handler = GetHandler(object);
    auto proxy = handler->GetProxy(object, nullptr);

    auto attributes = CreateEphemeralAttributes();
    yhash_set<TString> replicatedKeys;
    auto replicateKey = [&] (const TString& key, const TYsonString& value) {
        if (replicatedKeys.insert(key).second) {
            attributes->SetYson(key, value);
        }
    };

    // Check system attributes.
    std::vector<ISystemAttributeProvider::TAttributeDescriptor> descriptors;
    proxy->ListBuiltinAttributes(&descriptors);
    for (const auto& descriptor : descriptors) {
        if (!descriptor.Replicated) {
            continue;
        }

        if (descriptor.Mandatory != mandatory) {
            continue;
        }

        auto key = TString(descriptor.Key);
        auto value = proxy->FindBuiltinAttribute(key);
        if (value) {
            replicateKey(key, value);
        }
    }

    // Check custom attributes.
    const auto* customAttributes = object->GetAttributes();
    if (customAttributes) {
        for (const auto& pair : object->GetAttributes()->Attributes()) {
            replicateKey(pair.first, pair.second);
        }
    }
    return attributes;
}

void TObjectManager::OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
{
    auto schemas = GetValuesSortedByKey(SchemaMap_);
    for (auto* schema : schemas) {
        ReplicateObjectAttributesToSecondaryMaster(schema, cellTag);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

