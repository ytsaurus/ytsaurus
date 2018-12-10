#include "action.h"

#include <yt/ytlib/transaction_client/action.pb.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

void TTransactionActionData::Persist(TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Type);
    Persist(context, Value);
}

void ToProto(NProto::TTransactionActionData* protoData, const TTransactionActionData& data)
{
    protoData->set_action_type(data.Type);
    protoData->set_action_value(data.Value);
}

void FromProto(TTransactionActionData* data, const NProto::TTransactionActionData& protoData)
{
    data->Type = protoData.action_type();
    data->Value = protoData.action_value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient

