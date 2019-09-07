#include "helpers.h"
#include "transaction_manager.h"

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/api/transaction.h>

#include <yt/core/rpc/client.h>

namespace NYT::NTransactionClient {

using namespace NApi;
using namespace NRpc;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void SetTransactionId(IClientRequestPtr request, ITransactionPtr transaction)
{
    NCypressClient::SetTransactionId(
        request,
        transaction ? transaction->GetId() : NullTransactionId);
}

std::pair<TInstant, TInstant> TimestampToInstant(TTimestamp timestamp)
{
    auto lo = TInstant::Seconds(timestamp >> TimestampCounterWidth);
    auto hi = lo + TDuration::Seconds(1);
    return std::make_pair(lo, hi);
}

std::pair<TTimestamp, TTimestamp> InstantToTimestamp(TInstant instant)
{
    auto lo = instant.Seconds() << TimestampCounterWidth;
    auto hi = lo + (1 << TimestampCounterWidth);
    return std::make_pair(lo, hi);
}

std::pair<TDuration, TDuration> TimestampDiffToDuration(TTimestamp loTimestamp, TTimestamp hiTimestamp)
{
    YT_ASSERT(loTimestamp <= hiTimestamp);
    auto loInstant = TimestampToInstant(loTimestamp);
    auto hiInstant = TimestampToInstant(hiTimestamp);
    return std::make_pair(
        hiInstant.first >= loInstant.second ? hiInstant.first - loInstant.second : TDuration::Zero(),
        hiInstant.second - loInstant.first);
}

TTransactionId MakeTabletTransactionId(
    EAtomicity atomicity,
    TCellTag cellTag,
    TTimestamp startTimestamp,
    ui32 hash)
{
    EObjectType type;
    switch (atomicity) {
        case EAtomicity::Full:
            type = EObjectType::AtomicTabletTransaction;
            break;
        case EAtomicity::None:
            type = EObjectType::NonAtomicTabletTransaction;
            break;
        default:
            YT_ABORT();
    }

    return MakeId(
        type,
        cellTag,
        static_cast<ui64>(startTimestamp),
        hash);
}

TTransactionId MakeMirroredTransactionId(
    TTransactionId originalId,
    TCellTag mirrorCellTag)
{
    if (!originalId) {
        return {};
    }
    auto originalType = TypeFromId(originalId);
    YT_VERIFY(originalType == EObjectType::Transaction || originalType == EObjectType::NestedTransaction);
    auto mirroredType = (originalType == EObjectType::Transaction)
        ? EObjectType::MirroredTransaction
        : EObjectType::MirroredNestedTransaction;
    return TTransactionId(
        (originalId.Parts32[0] &  0xffff) | (mirrorCellTag << 16),           // insert mirror cell tag
        (originalId.Parts32[1] & ~0xffff) | static_cast<ui32>(mirroredType), // replace type
        originalId.Parts32[2],
        originalId.Parts32[3]);
}

TCellTag MirrorCellTagFromTransactionId(TTransactionId id)
{
    return id.Parts32[0] >> 16;
}

TTransactionId UnmirrorTransactionId(TTransactionId mirroredId)
{
    if (!mirroredId) {
        return {};
    }
    auto mirroredType = TypeFromId(mirroredId);
    YT_VERIFY(mirroredType == EObjectType::MirroredTransaction || mirroredType == EObjectType::MirroredNestedTransaction);
    auto originalType = (mirroredType == EObjectType::MirroredTransaction)
        ? EObjectType::Transaction
        : EObjectType::NestedTransaction;
    return TTransactionId(
        (mirroredId.Parts32[0] &  0xffff),                                   // erase mirror cell tag
        (mirroredId.Parts32[1] & ~0xffff) | static_cast<ui32>(originalType), // replace type
        mirroredId.Parts32[2],
        mirroredId.Parts32[3]);
}

TTimestamp TimestampFromTransactionId(TTransactionId id)
{
    return TTimestamp(CounterFromId(id));
}

EAtomicity AtomicityFromTransactionId(TTransactionId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::Transaction:
        case EObjectType::AtomicTabletTransaction:
            return EAtomicity::Full;

        case EObjectType::NonAtomicTabletTransaction:
            return EAtomicity::None;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient

