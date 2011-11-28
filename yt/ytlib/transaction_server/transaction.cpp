#include "stdafx.h"
#include "transaction.h"

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : Id_(id)
{ }

TAutoPtr<TTransaction> TTransaction::Clone() const
{
    return new TTransaction(*this);
}

void TTransaction::Save(TOutputStream* output) const
{
    YASSERT(output != NULL);

    //::Save(output, Id_);
    ::Save(output, RegisteredChunks_);
    ::Save(output, LockIds_);
    ::Save(output, BranchedNodes_);
}

TAutoPtr<TTransaction> TTransaction::Load(const TTransactionId& id, TInputStream* input)
{
    YASSERT(input != NULL);

    auto* transaction = new TTransaction(id);
    ::Load(input, transaction->RegisteredChunks_);
    ::Load(input, transaction->LockIds_);
    ::Load(input, transaction->BranchedNodes_);
    return transaction;
}

TTransaction::TTransaction(const TTransaction& other)
    : Id_(other.Id_)
    , RegisteredChunks_(other.RegisteredChunks_)
    , LockIds_(other.LockIds_)
    , BranchedNodes_(other.BranchedNodes_)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

