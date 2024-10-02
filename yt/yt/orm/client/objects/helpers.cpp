#include "helpers.h"

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

NClient::NObjects::TMasterInstanceTag InstanceTagFromTransactionId(TTransactionId transactionId)
{
    return static_cast<TMasterInstanceTag>(transactionId.Parts32[1] >> 24);
}

TClusterTag ClusterTagFromId(TTransactionId id)
{
    return static_cast<TClusterTag>((id.Parts32[1] >> 16) & 0x00ff);
}

TTransactionId GenerateTransactionId(
    TMasterInstanceTag instanceTag,
    TClusterTag clusterTag,
    TTimestamp timestamp)
{
    return TTransactionId(
        RandomNumber<ui32>(),
        (instanceTag.Underlying() << 24) + (clusterTag.Underlying() << 16) + (1U << 14),
        static_cast<ui32>(timestamp & 0xffffffff),
        static_cast<ui32>(timestamp >> 32));
}

bool IsValidInstanceTag(TMasterInstanceTag tag)
{
    return LowerMasterInstanceTag <= tag && tag < UpperMasterInstanceTag;
}

void ForEachValidInstanceTag(std::function<void(TMasterInstanceTag)> callback)
{
    for (auto tag = LowerMasterInstanceTag.Underlying(); tag < UpperMasterInstanceTag.Underlying(); ++tag) {
        callback(TMasterInstanceTag{tag});
    }
}

TString ValidInstanceTagRange()
{
    TStringBuilder builder;
    builder.AppendFormat("[%v, %v)", LowerMasterInstanceTag, UpperMasterInstanceTag);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
