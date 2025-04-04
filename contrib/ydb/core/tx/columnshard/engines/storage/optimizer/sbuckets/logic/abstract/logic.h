#pragma once
#include <contrib/ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <contrib/ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/common/optimizer.h>

#include <contrib/ydb/library/formats/arrow/replace_key.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

class TCalcWeightResult {
private:
    YDB_READONLY(ui64, Weight, 0);
    YDB_READONLY(TInstant, NextInstant, TInstant::Max());
public:
    TCalcWeightResult(const ui64 weight, const TInstant next)
        : Weight(weight)
        , NextInstant(next)
    {

    }
};

class TCompactionTaskResult {
private:
    YDB_READONLY_DEF(std::vector<TPortionInfo::TConstPtr>, Portions);
    YDB_READONLY_DEF(std::vector<NArrow::TReplaceKey>, SplitRightOpenIntervalPoints); // [-inf, p1), [p1, p2), ...
public:
    TCompactionTaskResult(std::vector<TPortionInfo::TConstPtr>&& portions, std::vector<NArrow::TReplaceKey>&& points)
        : Portions(std::move(portions))
        , SplitRightOpenIntervalPoints(std::move(points)) {

    }
};

class IOptimizationLogic {
private:
    virtual TCalcWeightResult DoCalcWeight(const TInstant now, const TBucketInfo& bucket) const = 0;
    virtual TCompactionTaskResult DoBuildTask(const TInstant now, const ui64 memLimit, const TBucketInfo& bucket) const = 0;
public:
    TCalcWeightResult CalcWeight(const TInstant now, const TBucketInfo& bucket) const {
        return DoCalcWeight(now, bucket);
    }

    TCompactionTaskResult BuildTask(const TInstant now, const ui64 memLimit, const TBucketInfo& bucket) const {
        return DoBuildTask(now, memLimit, bucket);
    }

    virtual ~IOptimizationLogic() = default;
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets
