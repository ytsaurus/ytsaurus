#ifndef COMPOSITE_META_STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include action.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
TBlob SerializeChange(
    const NRpcMasterStateManager::TMsgChangeHeader& header,
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
typename TAsyncResult<TResult>::TPtr TMetaStatePart::ApplyChange(
    const TMessage& message,
    TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod,
    IAction::TPtr errorHandler)
{
    typename TUpdate<TMessage, TResult>::TPtr update = new TUpdate<TMessage, TResult>(
        StateManager,
        GetPartName(),
        message,
        changeMethod,
        errorHandler);
    return update->Run();
}

template<class TMessage, class TResult>
void TMetaStatePart::RegisterMethod(TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod)
{
    Stroka changeType = TMessage().GetTypeName();
    IParamAction<const TRef&>::TPtr action = FromMethod(
        &TMetaStatePart::MethodThunk<TMessage, TResult>,
        this,
        changeMethod);
    YVERIFY(Methods.insert(MakePair(changeType, action)).Second() == 1);
}

template<class TMessage, class TResult>
void TMetaStatePart::MethodThunk(
    const TRef& changeData,
    typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod)
{
    google::protobuf::io::ArrayInputStream ais(changeData.Begin(), changeData.Size());
    TMessage message;
    YVERIFY(message.ParseFromZeroCopyStream(&ais));

    changeMethod->Do(message);
}

template<class TMessage, class TResult>
class TMetaStatePart::TUpdate
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TUpdate> TPtr;

    TUpdate(
        TMasterStateManager::TPtr stateManager,
        Stroka partName,
        const TMessage& message,
        typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod,
        IAction::TPtr errorHandler)
        : StateManager(stateManager)
        , PartName(partName)
        , Message(message)
        , ChangeMethod(changeMethod)
        , ErrorHandler(errorHandler)
        , AsyncResult(new TAsyncResult<TResult>())
    { }

    typename TAsyncResult<TResult>::TPtr Run()
    {
        // TODO: change ns
        NRpcMasterStateManager::TMsgChangeHeader header;
        header.SetPartName(PartName);
        header.SetChangeType(Message.GetTypeName());

        TBlob changeData = SerializeChange(header, Message);

        StateManager
            ->CommitChange(
                FromMethod(&TUpdate::InvokeChangeMethod, TPtr(this)),
                TSharedRef(changeData))
            ->Subscribe(
                FromMethod(&TUpdate::OnCommitted, TPtr(this)));

        return AsyncResult;
    }

private:
    void InvokeChangeMethod()
    {
        Result = ChangeMethod->Do(Message);
    }

    void OnCommitted(TMasterStateManager::ECommitResult commitResult)
    {
        if (commitResult == TMasterStateManager::ECommitResult::Committed) {
            AsyncResult->Set(Result);
        } else if (~ErrorHandler != NULL) {
            ErrorHandler->Do();
        }
    }

    TMasterStateManager::TPtr StateManager;
    Stroka PartName;
    TMessage Message;
    Stroka MethodName;
    typename IParamFunc<const TMessage&, TResult>::TPtr ChangeMethod;
    IAction::TPtr ErrorHandler;
    typename TAsyncResult<TResult>::TPtr AsyncResult;
    TResult Result;

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
