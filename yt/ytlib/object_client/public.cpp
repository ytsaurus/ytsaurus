#include "stdafx.h"
#include "public.h"

#include <core/misc/format.h>

#include <util/string/vector.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

TObjectId NullObjectId;
TTransactionId NullTransactionId;

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

TVersionedObjectId TVersionedObjectId::FromString(const TStringBuf& str)
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

void FormatValue(TStringBuilder* builder, const TVersionedObjectId& id)
{
    builder->AppendFormat("%v:%v", id.ObjectId, id.TransactionId);
}

Stroka ToString(const TVersionedObjectId& id)
{
    TStringBuilder builder;
    FormatValue(&builder, id);
    return builder.Flush();
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

} // namespace NObjectClient
} // namespace NYT

