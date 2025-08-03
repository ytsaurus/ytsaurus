#ifndef TRANSACTION_ACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_action.h"
// For the sake of sane code completion.
#include "transaction_action.h"
#endif

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/actions/bind.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction, class TProto, class TState>
std::string TTypedTransactionActionDescriptor<TTransaction, TProto, TState>::GetType()
{
    return TProto::default_instance().GetTypeName();
}

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <
    class TSaveContext,
    class TLoadContext,
    class TState
>
class TTypeErasedTransactionActionState
    : public ITransactionActionState<TSaveContext, TLoadContext>
{
public:
    void Save(TSaveContext& context) const override
    {
        NYT::Save(context, Underlying_);
    }

    void Load(TLoadContext& context) override
    {
        NYT::Load(context, Underlying_);
    }

    void* GetUnderlying() override
    {
        return &Underlying_;
    }

private:
    TState Underlying_;
};

template <
    class TSaveContext,
    class TLoadContext,
    class TState
>
std::function<std::unique_ptr<ITransactionActionState<TSaveContext, TLoadContext>>()>
MakeTypeErasedTransactionStateCreator()
{
    if constexpr(std::is_same_v<TState, void>) {
        return nullptr;
    } else {
        return [] () -> std::unique_ptr<ITransactionActionState<TSaveContext, TLoadContext>> {
            return std::make_unique<TTypeErasedTransactionActionState<TSaveContext, TLoadContext, TState>>();
        };
    }
}

template <
    class TTransaction,
    class TSaveContext,
    class TLoadContext,
    class TProto,
    class TState,
    class... TArgs
>
TCallback<void(
    TTransaction*,
    TStringBuf,
    ITransactionActionState<TSaveContext, TLoadContext>*,
    const TArgs&...)
> MakeTypeErasedTransactionActionHandler(auto handler)
{
    if (!handler) {
        return {};
    }

    return BIND_NO_PROPAGATE([handler = std::move(handler)] (
        TTransaction* transaction,
        TStringBuf value,
        ITransactionActionState<TSaveContext, TLoadContext>* state,
        const TArgs&... args)
    {
        TProto typedValue;
        DeserializeProto(&typedValue, TRef::FromStringBuf(value));
        if constexpr(std::is_same_v<TState, void>) {
            handler(transaction, &typedValue, args...);
        } else {
            auto* typedState = static_cast<TState*>(state->GetUnderlying());
            handler(transaction, &typedValue, typedState, args...);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class TTransaction, class TSaveContext, class TLoadContext>
template <class TProto, class TState>
TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext>::TTypeErasedTransactionActionDescriptor(
    TTypedTransactionActionDescriptor<TTransaction, TProto, TState> descriptor)
    : Type_(descriptor.GetType())
    , CreateState_(NDetail::MakeTypeErasedTransactionStateCreator<TSaveContext, TLoadContext, TState>())
    , Prepare_(NDetail::MakeTypeErasedTransactionActionHandler<TTransaction, TSaveContext, TLoadContext, TProto, TState, const TTransactionPrepareOptions&>(
        std::move(descriptor.Prepare)))
    , Commit_(NDetail::MakeTypeErasedTransactionActionHandler<TTransaction, TSaveContext, TLoadContext, TProto, TState, const TTransactionCommitOptions&>(
        std::move(descriptor.Commit)))
    , Abort_(NDetail::MakeTypeErasedTransactionActionHandler<TTransaction, TSaveContext, TLoadContext, TProto, TState, const TTransactionAbortOptions&>(
        std::move(descriptor.Abort)))
    , Serialize_(NDetail::MakeTypeErasedTransactionActionHandler<TTransaction, TSaveContext, TLoadContext, TProto, TState>(
        std::move(descriptor.Serialize)))
{ }

template <class TTransaction, class TSaveContext, class TLoadContext>
auto TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext>::CreateState() const
    -> std::unique_ptr<IActionState>
{
    return CreateState_ ? CreateState_() : nullptr;
}

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext>::Prepare(
    TTransaction* transaction,
    TStringBuf value,
    IActionState* state,
    const TTransactionPrepareOptions& options) const
{
    if (Prepare_) {
        Prepare_(transaction, value, state, options);
    }
}

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext>::Commit(
    TTransaction* transaction,
    TStringBuf value,
    IActionState* state,
    const TTransactionCommitOptions& options) const
{
    if (Commit_) {
        Commit_(transaction, value, state, options);
    }
}

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext>::Abort(
    TTransaction* transaction,
    TStringBuf value,
    IActionState* state,
    const TTransactionAbortOptions& options) const
{
    if (Abort_) {
        Abort_(transaction, value, state, options);
    }
}

template <class TTransaction, class TSaveContext, class TLoadContext>
void TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext>::Serialize(
    TTransaction* transaction,
    TStringBuf value,
    IActionState* state) const
{
    if (Serialize_) {
        return Serialize_(transaction, value, state);
    }
}

template <class TTransaction, class TSaveContext, class TLoadContext>
bool TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext>::HasSerializeHandler() const
{
    return static_cast<bool>(Serialize_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
