#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionActionData
{
    //! Protobuf message type.
    Stroka Type;
    //! Protobuf-encoded value.
    Stroka Value;

    void Persist(TStreamPersistenceContext& context);
};

void ToProto(NProto::TTransactionActionData* protoData, const TTransactionActionData& data);
void FromProto(TTransactionActionData* data, const NProto::TTransactionActionData& protoData);

template <class TProto>
TTransactionActionData MakeTransactionActionData(const TProto& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT

#define ACTION_INL_H_
#include "action-inl.h"
#undef ACTION_INL_H_
