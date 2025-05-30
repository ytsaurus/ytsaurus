#pragma once
#include <contrib/ydb/library/accessor/positive_integer.h>
#include <contrib/ydb/library/actors/core/monotonic.h>
#include <contrib/ydb/core/tx/columnshard/common/path_id.h>

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

#include <memory>

namespace NKikimr::NOlap {
class TGranuleMeta;

class TPlanCompactionInfo {
private:
    TInternalPathId PathId;
    TMonotonic StartTime = TMonotonic::Now();
    TPositiveControlInteger Count;

public:
    void Start() {
        StartTime = TMonotonic::Now();
        ++Count;
    }

    bool Finish();

    TMonotonic GetStartTime() const {
        return StartTime;
    }

    explicit TPlanCompactionInfo(const TInternalPathId pathId)
        : PathId(pathId) {
    }

    TInternalPathId GetPathId() const {
        return PathId;
    }
};

}   // namespace NKikimr::NOlap
