#pragma once

#include <contrib/ydb/core/kqp/gateway/behaviour/streaming_query/common/utils.h>
#include <contrib/ydb/services/metadata/abstract/kqp_common.h>
#include <contrib/ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NKqp {

class TStreamingQueryConfig : public TStreamingQueryMeta {
public:
    using TStatus = NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
    using TAsyncStatus = NThreading::TFuture<TStatus>;

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();

    static TString GetTypeId();
};

}  // namespace NKikimr::NKqp
