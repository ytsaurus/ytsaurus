#include "stdafx.h"
#include "transaction.h"

#include <util/ysaveload.h>

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

    ::Save(output, RegisteredChunks_);
    ::Save(output, LockIds_);
    ::Save(output, BranchedNodes_);
    ::Save(output, CreatedNodes_);
}

TAutoPtr<TTransaction> TTransaction::Load(const TTransactionId& id, TInputStream* input)
{
    YASSERT(input != NULL);

    auto* transaction = new TTransaction(id);
    ::Load(input, transaction->RegisteredChunks_);
    ::Load(input, transaction->LockIds_);
    ::Load(input, transaction->BranchedNodes_);
    ::Load(input, transaction->CreatedNodes_);
    return transaction;
}

TTransaction::TTransaction(const TTransaction& other)
    : Id_(other.Id_)
    , RegisteredChunks_(other.RegisteredChunks_)
    , LockIds_(other.LockIds_)
    , BranchedNodes_(other.BranchedNodes_)
    , CreatedNodes_(other.CreatedNodes_)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

