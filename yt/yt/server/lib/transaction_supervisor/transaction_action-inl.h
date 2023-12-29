#ifndef TRANSACTION_ACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_action.h"
// For the sake of sane code completion.
#include "transaction_action.h"
#endif

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction, class TProto>
TString TTypedTransactionActionDescriptor<TTransaction, TProto>::GetType()
{
    return TProto::default_instance().GetTypeName();
}

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction, class TProto, class... TArgs>
TCallback<void(TTransaction*, const TString&, const TArgs&...)>
MakeTypeErasedTransactionActionHandler(TCallback<void(TTransaction*, TProto*, const TArgs&... args)> handler)
{
    if (!handler) {
        return {};
    }

    return BIND_NO_PROPAGATE([
        handler = std::move(handler)
    ] (TTransaction* transaction, const TString& value, const TArgs&... args) {
        TProto typedValue;
        DeserializeProto(&typedValue, TRef::FromString(value));
        handler(transaction, &typedValue, args...);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class TTransaction>
template <class TProto>
TTransactionActionDescriptor<TTransaction>::TTransactionActionDescriptor(
    TTypedTransactionActionDescriptor<TTransaction, TProto> descriptor)
    : Type_(descriptor.GetType())
    , Prepare_(NDetail::MakeTypeErasedTransactionActionHandler(std::move(descriptor.Prepare)))
    , Commit_(NDetail::MakeTypeErasedTransactionActionHandler(std::move(descriptor.Commit)))
    , Abort_(NDetail::MakeTypeErasedTransactionActionHandler(std::move(descriptor.Abort)))
    , Serialize_(NDetail::MakeTypeErasedTransactionActionHandler(std::move(descriptor.Serialize)))
{ }

template <class TTransaction>
void TTransactionActionDescriptor<TTransaction>::Prepare(
    TTransaction* transaction,
    const TString& value,
    const TTransactionPrepareOptions& options) const
{
    if (Prepare_) {
        Prepare_(transaction, value, options);
    }
}

template <class TTransaction>
void TTransactionActionDescriptor<TTransaction>::Commit(
    TTransaction* transaction,
    const TString& value,
    const TTransactionCommitOptions& options) const
{
    if (Commit_) {
        Commit_(transaction, value, options);
    }
}

template <class TTransaction>
void TTransactionActionDescriptor<TTransaction>::Abort(
    TTransaction* transaction,
    const TString& value,
    const TTransactionAbortOptions& options) const
{
    if (Abort_) {
        Abort_(transaction, value, options);
    }
}

template <class TTransaction>
void TTransactionActionDescriptor<TTransaction>::Serialize(
    TTransaction* transaction,
    const TString& value) const
{
    if (Serialize_) {
        return Serialize_(transaction, value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
