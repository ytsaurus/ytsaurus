#include "stdafx.h"
#include "object_manager.h"

#include <util/digest/murmur.h>

namespace NYT {
namespace NObjectServer {

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
    // No thread affinity is given here.
    // This will be called during init-time only.

    YASSERT(handler);
    int typeValue = handler->GetType().ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);
    YASSERT(!TypeToHandler[typeValue]);
    TypeToHandler[typeValue] = handler;
}

IObjectTypeHandler* TObjectManager::GetHandler(EObjectType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int typeValue = type.ToValue();
    YASSERT(typeValue >= 0 && typeValue < MaxObjectType);
    auto handler = TypeToHandler[typeValue];
    YASSERT(handler);
    return ~handler;
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

    ui64 counter = Counter++;

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
    auto counter = Counter;
    return
        FromFunctor([=] () -> TVoid
            {
                ::Save(output, counter);
                return TVoid();
            })
        ->AsyncVia(context.Invoker)
        ->Do();
}

void TObjectManager::Load(TInputStream* input)
{
    VERIFY_THREAD_AFFINITY(StateThread);

    ::Load(input, Counter);
}

void TObjectManager::Clear()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    Counter = 0;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

