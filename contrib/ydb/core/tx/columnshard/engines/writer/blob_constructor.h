#pragma once

#include <contrib/ydb/core/protos/base.pb.h>
#include <contrib/ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <contrib/ydb/library/accessor/accessor.h>

#include <contrib/ydb/library/actors/core/event.h>

namespace NKikimr {

struct TAppData;

namespace NColumnShard {

class TBlobBatch;
struct TUsage;

}

namespace NOlap {

class TBlobWriteInfo {
private:
    YDB_READONLY_DEF(TUnifiedBlobId, BlobId);
    YDB_READONLY_DEF(TString, Data);
    YDB_ACCESSOR_DEF(std::shared_ptr<IBlobsWritingAction>, WriteOperator);

    TBlobWriteInfo(const TString& data, const std::shared_ptr<IBlobsWritingAction>& writeOperator, const std::optional<TUnifiedBlobId>& customBlobId);
public:
    static TBlobWriteInfo BuildWriteTask(const TString& data, const std::shared_ptr<IBlobsWritingAction>& writeOperator, const std::optional<TUnifiedBlobId>& customBlobId = {});
};

}
}
