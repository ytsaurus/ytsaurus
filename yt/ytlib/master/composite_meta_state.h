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
    typedef TIntrusivePtr<TMetaStateServiceBase> TPtr;

    TMetaStateServiceBase(
        IInvoker::TPtr serviceInvoker,
        Stroka serviceName,
        Stroka loggingCategory);

    template <
        class TState,
        class TMessage,
        class TThis,
        class TResult,
        class TContext
    >
    static void CommitChange(
        TThis* this_,
        TContext context,
        TIntrusivePtr<TState> state,
        const TMessage& message,
        TResult (TState::* changeMethod)(const TMessage&),
        void (TThis::* handlerMethod)(TResult result, TContext context))
    {
        state
            ->CommitChange(
                message,
                FromMethod(changeMethod, state),
                FromMethod(&TMetaStateServiceBase::OnCommitError, this_, context->GetUntypedContext()))
            ->Subscribe(
                FromMethod(handlerMethod, this_, context));
    }

    template <
        class TState,
        class TMessage,
        class TThis,
        class TResult,
        class TContext
    >
    static void CommitChange(
        TThis* this_,
        TContext context,
        TIntrusivePtr<TState> state,
        const TMessage& message,
        TResult (TState::* changeMethod)(const TMessage&))
    {
        TIntrusivePtr<TThis> intrusiveThis(this_);
        NRpc::TServiceContext::TPtr untypedContext(context->GetUntypedContext());
        state
            ->CommitChange(
                message,
                FromMethod(changeMethod, state),
                FromMethod(&TMetaStateServiceBase::OnCommitError, intrusiveThis, untypedContext))
            ->Subscribe(
                FromMethod(&TMetaStateServiceBase::OnCommitSuccess<TResult>, intrusiveThis, untypedContext));
    }

private:
    void OnCommitError(NRpc::TServiceContext::TPtr context)
    {
        context->Reply(NRpc::EErrorCode::ServiceError);
    }

    template<class TResult>
    void OnCommitSuccess(TResult, NRpc::TServiceContext::TPtr context)
    {
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeMetaState;

class TMetaStatePart
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMetaStatePart> TPtr;

    template<class TMessage, class TResult>
    typename TAsyncResult<TResult>::TPtr CommitChange(
        const TMessage& message,
        TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod,
        IAction::TPtr errorHandler = NULL);

protected:
    TMetaStatePart(TMasterStateManager::TPtr stateManager);

    template<class TMessage, class TResult>
    void RegisterMethod(TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod);

    template<class TThis, class TMessage, class TResult>
    void RegisterMethod(
        TThis* this_,
        TResult (TThis::* changeMethod)(const TMessage&))
    {
        RegisterMethod(FromMethod(changeMethod, this_));
    }

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

    void CommitChange(Stroka changeType, TRef changeData);

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

} // namespace NYT

#define COMPOSITE_META_STATE_INL_H_
#include "composite_meta_state-inl.h"
#undef COMPOSITE_META_STATE_INL_H_
