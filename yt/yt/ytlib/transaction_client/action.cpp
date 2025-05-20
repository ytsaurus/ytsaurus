#include "action.h"

#include <yt/yt/ytlib/transaction_client/proto/action.pb.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

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

