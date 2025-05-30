#pragma once
#include "abstract.h"
#include <contrib/ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <contrib/ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <contrib/ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NDataLocks {

class TSnapshotLock: public ILock {
private:
    using TBase = ILock;
    const TSnapshot SnapshotBarrier;
    const THashSet<TInternalPathId> PathIds;
protected:
    virtual std::optional<TString> DoIsLocked(
        const TPortionInfo& portion, const ELockCategory /*category*/, const THashSet<TString>& /*excludedLocks*/) const override {
        if (PathIds.contains(portion.GetPathId()) && portion.RecordSnapshotMin() <= SnapshotBarrier) {
            return GetLockName();
        }
        return {};
    }
    virtual bool DoIsEmpty() const override {
        return PathIds.empty();
    }
    virtual std::optional<TString> DoIsLocked(
        const TGranuleMeta& granule, const ELockCategory /*category*/, const THashSet<TString>& /*excludedLocks*/) const override {
        if (PathIds.contains(granule.GetPathId())) {
            return GetLockName();
        }
        return {};
    }
public:
    TSnapshotLock(const TString& lockName, const TSnapshot& snapshotBarrier, const THashSet<TInternalPathId>& pathIds, const ELockCategory category, const bool readOnly = false)
        : TBase(lockName, category, readOnly)
        , SnapshotBarrier(snapshotBarrier)
        , PathIds(pathIds)
    {
        AFL_VERIFY(SnapshotBarrier.Valid());
    }
};

}