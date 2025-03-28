#pragma once
#include <contrib/ydb/core/tx/columnshard/splitter/abstract/chunks.h>

namespace NKikimr::NOlap::NCompaction {

class TColumnPortionResult {
protected:
    std::vector<std::shared_ptr<IPortionDataChunk>> Chunks;
    const ui32 ColumnId;
public:

    ui32 GetRecordsCount() const;

    TColumnPortionResult(const ui32 columnId)
        : ColumnId(columnId) {

    }

    const std::vector<std::shared_ptr<IPortionDataChunk>>& GetChunks() const {
        return Chunks;
    }

    TString DebugString() const;

};

}
