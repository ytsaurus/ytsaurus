#pragma once

#include "master_state_manager.h"

#include "../rpc/service.h"
#include "../rpc/server.h"

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateServiceBase
    : public NRpc::TServiceBase
{
protected:
    TMetaStateServiceBase(
        IInvoker::TPtr serviceInvoker,
        Stroka serviceName,
        Stroka loggingCategory);

    void OnCommitError(NRpc::TServiceContext::TPtr context);
};

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TFixedChangeHeader
{
    i32 HeaderSize;
    i32 MessageSize;
};

#pragma pack(pop)

template <class TMessage>
TBlob SerializeChange(
    const NRpcMasterStateManager::TMsgChangeHeader& header,
    const TMessage& message);

void DeserializeChangeHeader(
    TRef changeData,
    NRpcMasterStateManager::TMsgChangeHeader* header);

void DeserializeChange(
    TRef changeData,
    NRpcMasterStateManager::TMsgChangeHeader* header,
    TRef* messageData);

////////////////////////////////////////////////////////////////////////////////

class TCompositeMetaState;

class TMetaStatePart
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMetaStatePart> TPtr;

    template<class TMessage, class TResult>
    typename TAsyncResult<TResult>::TPtr ApplyChange(
        const TMessage& message,
        TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod,
        IAction::TPtr errorHandler = NULL);

protected:
    TMetaStatePart(TMasterStateManager::TPtr stateManager);

    template<class TMessage, class TResult>
    void RegisterMethod(TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod);

    bool IsLeader() const;
    bool IsFolllower() const;

    virtual Stroka GetPartName() const = 0;
    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& output) = 0;
    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& input) = 0;
    virtual void Clear() = 0;

    TMasterStateManager::TPtr StateManager;
    IInvoker::TPtr SnapshotInvoker;

private:
    friend class TCompositeMetaState;

    void OnRegistered(IInvoker::TPtr snapshotInvoker);

    void ApplyChange(Stroka changeType, TRef changeData);

    template<class TMessage, class TResult>
    void MethodThunk(
        const TRef& changeData,
        typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod);

    template<class TMessage, class TResult>
    class TUpdate;

    typedef yhash_map<Stroka, IParamAction<const TRef&>::TPtr> TMethodMap;
    TMethodMap Methods;

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeMetaState
    : public IMasterState 
{
public:
    typedef TIntrusivePtr<TCompositeMetaState> TPtr;

    TCompositeMetaState();

    void RegisterPart(TMetaStatePart::TPtr part);

    virtual IInvoker::TPtr GetInvoker() const;

private:
    IInvoker::TPtr StateInvoker;
    IInvoker::TPtr SnapshotInvoker;

    typedef yhash_map<Stroka, TMetaStatePart::TPtr> TPartMap;
    TPartMap Parts;

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& output);
    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& input);
    virtual void ApplyChange(TRef changeData);
    virtual void Clear();
};

////////////////////////////////////////////////////////////////////////////////

#define METASTATE_REGISTER_METHOD(method) \
    RegisterMethod(FromMethod(&TThis::method, this))

#define METASTATE_APPLY_RPC_CHANGE(method, handler) \
    State \
        ->ApplyChange( \
            message, \
            FromMethod(&TState::method, State), \
            FromMethod(&TThis::OnCommitError, this, context->GetUntypedContext())) \
        ->Subscribe( \
            FromMethod(&TThis::handler, this, context))

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define COMPOSITE_META_STATE_INL_H_
#include "composite_meta_state-inl.h"
#undef COMPOSITE_META_STATE_INL_H_
