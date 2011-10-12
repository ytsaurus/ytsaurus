#ifndef COMPOSITE_META_STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_meta_state.h"
#endif

#include "composite_meta_state_detail.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
TBlob SerializeChange(
    const NMetaState::NProto::TMsgChangeHeader& header,
    const TMessage& message)
{
    TFixedChangeHeader fixedHeader;
    fixedHeader.HeaderSize = header.ByteSize();
    fixedHeader.MessageSize = message.ByteSize();

    TBlob data(sizeof (TFixedChangeHeader) + fixedHeader.HeaderSize + fixedHeader.MessageSize);

    Copy(
        reinterpret_cast<char*>(&fixedHeader),
        sizeof (TFixedChangeHeader),
        data.begin());
    YVERIFY(header.SerializeToArray(
        data.begin() + sizeof (TFixedChangeHeader),
        fixedHeader.HeaderSize));
    YVERIFY(message.SerializeToArray(
        data.begin() + sizeof (TFixedChangeHeader) + fixedHeader.HeaderSize,
        fixedHeader.MessageSize));
    return data;
}

////////////////////////////////////////////////////////////////////////////////

template<class TMessage, class TResult>
typename TFuture<TResult>::TPtr TMetaStatePart::CommitChange(
    const TMessage& message,
    TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod,
    IAction::TPtr errorHandler,
    ECommitMode mode)
{
    YASSERT(~changeMethod != NULL);

    return
        New< TUpdate<TMessage, TResult> >(
            MetaStateManager,
            GetPartName(),
            message,
            changeMethod,
            errorHandler,
            mode)
        ->Run();
}

template<class TMessage, class TResult>
void TMetaStatePart::RegisterMethod(TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod)
{
    YASSERT(~changeMethod != NULL);

    Stroka changeType = TMessage().GetTypeName();
    auto action = FromMethod(
        &TMetaStatePart::MethodThunk<TMessage, TResult>,
        this,
        changeMethod);
    YVERIFY(MetaState->Methods.insert(MakePair(changeType, action)).Second() == 1);
}

template<class TMessage, class TResult>
void TMetaStatePart::MethodThunk(
    const TRef& changeData,
    typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod)
{
    YASSERT(~changeMethod != NULL);

    TMessage message;
    YVERIFY(message.ParseFromArray(changeData.Begin(), changeData.Size()));

    changeMethod->Do(message);
}

template<class TMessage, class TResult>
class TMetaStatePart::TUpdate
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TUpdate> TPtr;

    TUpdate(
        TMetaStateManager::TPtr stateManager,
        Stroka partName,
        const TMessage& message,
        typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod,
        IAction::TPtr errorHandler,
        ECommitMode mode)
        : StateManager(stateManager)
        , PartName(partName)
        , Message(message)
        , ChangeMethod(changeMethod)
        , ErrorHandler(errorHandler)
        , Mode(mode)
        , AsyncResult(New< TFuture<TResult> >())
    {
        YASSERT(~stateManager != NULL);
        YASSERT(~changeMethod != NULL);
    }

    typename TFuture<TResult>::TPtr Run()
    {
        NProto::TMsgChangeHeader header;
        header.SetChangeType(Message.GetTypeName());

        auto changeData = SerializeChange(header, Message);

        StateManager
            ->CommitChangeSync(
                FromMethod(&TUpdate::InvokeChangeMethod, TPtr(this)),
                TSharedRef(changeData),
                Mode)
            ->Subscribe(
                FromMethod(&TUpdate::OnCommitted, TPtr(this)));

        return AsyncResult;
    }

private:
    void InvokeChangeMethod()
    {
        Result = ChangeMethod->Do(Message);
    }

    void OnCommitted(ECommitResult commitResult)
    {
        if (commitResult == ECommitResult::Committed) {
            AsyncResult->Set(Result);
        } else if (~ErrorHandler != NULL) {
            ErrorHandler->Do();
        }
    }

    TMetaStateManager::TPtr StateManager;
    Stroka PartName;
    TMessage Message;
    Stroka MethodName;
    typename IParamFunc<const TMessage&, TResult>::TPtr ChangeMethod;
    IAction::TPtr ErrorHandler;
    ECommitMode Mode;
    typename TFuture<TResult>::TPtr AsyncResult;
    TResult Result;

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
