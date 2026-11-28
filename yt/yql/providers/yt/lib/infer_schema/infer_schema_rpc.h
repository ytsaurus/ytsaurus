#pragma once
#include "infer_schema.h"

#include <yt/yql/providers/yt/lib/threading/async_queue.h>

namespace NYql {
struct TTableInferSchemaRequest {
    const TString& TableId;
    const TString& TableName;
    ui32 Rows;
};
TVector<TMaybe<NYT::TNode>> InferSchemaFromTablesContents(const TString& cluster, const TString& token, const NYT::TTransactionId& tx, const std::vector<TTableInferSchemaRequest>& requests, TAsyncQueue::TWeakPtr queue);
}
