#include "stdafx.h"
#include "object_manager.h"

#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/transaction_server/transaction.h>

#include <ytlib/ytree/serialize.h>

#include <util/digest/murmur.h>

namespace NYT {
namespace NObjectServer {

using namespace NYTree;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(ObjectServerLogger);

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(
    IMetaStateManager* metaStateManager,
    TCompositeMetaState* metaState,
    TCellId cellId)
    : TMetaStatePart(metaStateManager, metaState)
    , CellId(cellId)
    , TypeToHandler(MaxObjectType)
    , TypeToCounter(MaxObjectType)
{
    metaState->RegisterLoader(
        "ObjectManager.1",
        FromMethod(&TObjectManager::Load, TPtr(this)));
    metaState->RegisterSaver(
        "ObjectManager.1",
        FromMethod(&TObjectManager::Save, TPtr(this)));

    metaState->RegisterPart(this);

    LOG_INFO("Object Manager initialized (CellId: %d)",
        static_cast<int>(cellId));
}

void TObjectManager::RegisterHandler(IObjectTypeHandler* handler)
{
    // No thread affinity check here.
    // This will be called during init-time only but from an unspecified thread.
    YASSERT(handler);
    int typeValue = handler->GetType().ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);
    YASSERT(!TypeToHandler[typeValue]);
    TypeToHandler[typeValue] = handler;
    TypeToCounter[typeValue] = TIdGenerator<ui64>();
}

IObjectTypeHandler* TObjectManager::FindHandler( EObjectType type ) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = type.ToValue();
    if (typeValue < 0 || typeValue >= MaxObjectType) {
        return NULL;
    }

    return ~TypeToHandler[typeValue];
}

IObjectTypeHandler* TObjectManager::GetHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto handler = FindHandler(type);
    YASSERT(handler);
    return handler;
}

IObjectTypeHandler* TObjectManager::GetHandler(const TObjectId& id) const
{
    return GetHandler(TypeFromId(id));
}

TCellId TObjectManager::GetCellId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CellId;
}

TObjectId TObjectManager::GenerateId(EObjectType type)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);

    ui64 counter = TypeToCounter[typeValue].Next();

    char data[12];
    *reinterpret_cast<ui64*>(&data[ 0]) = counter;
    *reinterpret_cast<ui16*>(&data[ 8]) = typeValue;
    *reinterpret_cast<ui16*>(&data[10]) = CellId;
    ui32 hash = MurmurHash<ui32>(&data, sizeof (data), 0);

    TObjectId id(
        hash,
        (CellId << 16) + type.ToValue(),
        counter & 0xffffffff,
        counter >> 32);

    LOG_DEBUG_IF(!IsRecovery(), "Object id generated (Type: %s, Id: %s)",
        ~type.ToString(),
        ~id.ToString());

    return id;
}

void TObjectManager::RefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    i32 refCounter = GetHandler(id)->RefObject(id);
    LOG_DEBUG_IF(!IsRecovery(), "Object referenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
}

void TObjectManager::UnrefObject(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto handler = GetHandler(id);
    i32 refCounter = handler->UnrefObject(id);
    LOG_DEBUG_IF(!IsRecovery(), "Object unreferenced (Id: %s, RefCounter: %d)",
        ~id.ToString(),
        refCounter);
    if (refCounter == 0) {
        LOG_DEBUG_IF(!IsRecovery(), "Object destroyed (Type: %s, Id: %s)",
            ~handler->GetType().ToString(),
            ~id.ToString());
    }
}

i32 TObjectManager::GetObjectRefCounter(const TObjectId& id)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    return GetHandler(id)->GetObjectRefCounter(id);
}

TFuture<TVoid>::TPtr TObjectManager::Save(const TCompositeMetaState::TSaveContext& context)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto* output = context.Output;
    auto invoker = context.Invoker;

    auto typeToCounter = TypeToCounter;
    invoker->Invoke(FromFunctor([=] ()
        {
            ::Save(output, typeToCounter);
        }));
    
    return Attributes.Save(invoker, output);
}

void TObjectManager::Load(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ::Load(input, TypeToCounter);
    Attributes.Load(input);
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    for (int i = 0; i < MaxObjectType; ++i) {
        TypeToCounter[i].Reset();
    }
}

IObjectProxy::TPtr TObjectManager::FindProxy(const TObjectId& id)
{
    auto type = TypeFromId(id);
    int typeValue = type.ToValue();
    if (typeValue < 0 || typeValue >= MaxObjectType) {
        return NULL;
    }

    auto handler = TypeToHandler[typeValue];
    if (!handler) {
        return NULL;
    }

    if (!handler->Exists(id)) {
        return NULL;
    }

    return handler->GetProxy(id);
}

IObjectProxy::TPtr TObjectManager::GetProxy(const TObjectId& id)
{
    auto proxy = FindProxy(id);
    YASSERT(proxy);
    return proxy;
}


TAttributeSet* TObjectManager::CreateAttributes(const TVersionedObjectId& id)
{
    auto result = new TAttributeSet();
    Attributes.Insert(id, result);
    return result;
}

void TObjectManager::AddAttributes(const TVersionedObjectId& id, IMapNode* value)
{
    if (value->GetChildCount() == 0)
        return;

    auto* attributes = FindAttributesForUpdate(id);
    if (!attributes) {
        attributes = CreateAttributes(id);
    }

    FOREACH (const auto& pair, value->GetChildren()) {
        const auto& key = pair.first;
        auto value = SerializeToYson(~pair.second);
        YVERIFY(attributes->Attributes().insert(MakePair(key, value)).second);
    }
}

void TObjectManager::RemoveAttributes(const TVersionedObjectId& id)
{
    Attributes.Remove(id);
}

IMapNode::TPtr TObjectManager::GetAttributesMap(const TVersionedObjectId& id) const
{
    auto map = GetEphemeralNodeFactory()->CreateMap();
    const auto* attributes = FindAttributes(id);
    if (!attributes) {
        return map;
    }
    FOREACH (const auto& pair, attributes->Attributes()) {
        auto value = DeserializeFromYson(pair.second);
        map->AddChild(~value, pair.first);
    }
    return map;
}

DEFINE_METAMAP_ACCESSORS(TObjectManager, Attributes, TAttributeSet, TVersionedObjectId, Attributes)

void TObjectManager::BranchAttributes(
    const TVersionedObjectId& originatingId,
    const TVersionedObjectId& branchedId)
{
    UNUSED(originatingId);
    UNUSED(branchedId);
    // We don't store empty deltas at the moment
}

void TObjectManager::MergeAttributes(
    const TVersionedObjectId& originatingId,
    const TVersionedObjectId& branchedId)
{
    auto* originatingAttributes = FindAttributesForUpdate(originatingId);
    const auto* branchedAttributes = FindAttributes(branchedId);
    if (!branchedAttributes) {
        return;
    }
    if (!originatingAttributes) {
        Attributes.Insert(originatingId, ~branchedAttributes->Clone());
    } else {
        FOREACH (const auto& pair, branchedAttributes->Attributes()) {
            if (pair.second.empty() && !originatingId.IsBranched()) {
                originatingAttributes->Attributes().erase(pair.first);
            } else {
                originatingAttributes->Attributes()[pair.first] = pair.second;
            }
        }
    }
    Attributes.Remove(branchedId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

