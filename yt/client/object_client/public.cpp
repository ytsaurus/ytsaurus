#include "public.h"

#include <yt/core/misc/format.h>

#include <util/string/vector.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

const TObjectId NullObjectId;
const TTransactionId NullTransactionId;

////////////////////////////////////////////////////////////////////////////////

TVersionedObjectId::TVersionedObjectId()
{ }

TVersionedObjectId::TVersionedObjectId(const TObjectId& objectId)
    : ObjectId(objectId)
{ }

TVersionedObjectId::TVersionedObjectId(
    const TObjectId& objectId,
    const TTransactionId& transactionId)
    : ObjectId(objectId)
    , TransactionId(transactionId)
{ }

bool TVersionedObjectId::IsBranched() const
{
    return TransactionId.operator bool();
}

TVersionedObjectId TVersionedObjectId::FromString(TStringBuf str)
{
    TStringBuf objectToken, transactionToken;
    str.Split(':', objectToken, transactionToken);

    auto objectId = TObjectId::FromString(objectToken);
    auto transactionId =
        transactionToken.empty()
        ? NullTransactionId
        : TTransactionId::FromString(transactionToken);
    return TVersionedObjectId(objectId, transactionId);
}

void FormatValue(TStringBuilder* builder, const TVersionedObjectId& id, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v:%v", id.ObjectId, id.TransactionId);
}

TString ToString(const TVersionedObjectId& id)
{
    return ToStringViaBuilder(id);
}

bool operator == (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TVersionedObjectId)) == 0;
}

bool operator != (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs)
{
    return !(lhs == rhs);
}

bool operator < (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (TVersionedObjectId)) < 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

