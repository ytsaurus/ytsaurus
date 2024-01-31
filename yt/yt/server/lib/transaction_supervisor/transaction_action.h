#pragma once

#include "public.h"

#include <yt/yt/ytlib/transaction_client/action.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction, class TProto>
struct TTypedTransactionActionDescriptor
{
    TCallback<void(TTransaction*, TProto*, const TTransactionPrepareOptions&)> Prepare;
    TCallback<void(TTransaction*, TProto*, const TTransactionCommitOptions&)> Commit;
    TCallback<void(TTransaction*, TProto*, const TTransactionAbortOptions&)> Abort;
    TCallback<void(TTransaction*, TProto*)> Serialize;

    static TString GetType();
};

template <class TTransaction>
class TTransactionActionDescriptor
{
public:
    DEFINE_BYREF_RO_PROPERTY(TString, Type);

public:
    template <class TProto>
    explicit TTransactionActionDescriptor(
        TTypedTransactionActionDescriptor<TTransaction, TProto> descriptor);

    void Prepare(TTransaction* transaction, const TString& value, const TTransactionPrepareOptions& options) const;
    void Commit(TTransaction* transaction, const TString& value, const TTransactionCommitOptions& options) const;
    void Abort(TTransaction* transaction, const TString& value, const TTransactionAbortOptions& options) const;
    void Serialize(TTransaction* transaction, const TString& value) const;

private:
    const TCallback<void(TTransaction*, const TString&, const TTransactionPrepareOptions&)> Prepare_;
    const TCallback<void(TTransaction*, const TString&, const TTransactionCommitOptions&)> Commit_;
    const TCallback<void(TTransaction*, const TString&, const TTransactionAbortOptions&)> Abort_;
    const TCallback<void(TTransaction*, const TString&)> Serialize_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor

#define TRANSACTION_ACTION_INL_H_
#include "transaction_action-inl.h"
#undef TRANSACTION_ACTION_INL_H_
