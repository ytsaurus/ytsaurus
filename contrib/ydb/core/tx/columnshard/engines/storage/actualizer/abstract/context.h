#pragma once
#include <contrib/ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <contrib/ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NOlap::NActualizer {

class TTieringProcessContext;

class TActualizationContext {
private:
    YDB_READONLY_DEF(TInstant, Now);

public:
    TActualizationContext(const TInstant now)
        : Now(now) {
    }
};

class TActualizationBuildingContext {
private:
    YDB_READONLY_DEF(TInstant, Now);
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& Portions;

public:
    TActualizationBuildingContext(const TInstant now, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions)
        : Now(now)
        , Portions(portions) {
    }

    const std::shared_ptr<TPortionInfo>& GetPortionVerified(const ui64 portionId) const {
        auto it = Portions.find(portionId);
        AFL_VERIFY(it != Portions.end());
        return it->second;
    }
};

class TAddExternalContext {
private:
    YDB_READONLY_DEF(TInstant, Now);
    YDB_ACCESSOR(bool, PortionExclusiveGuarantee, true);
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& Portions;
public:
    TAddExternalContext(const TInstant now, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions)
        : Now(now)
        , Portions(portions)
    {

    }

    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetPortions() const {
        return Portions;
    }
};

class TExternalTasksContext {
private:
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& Portions;
public:
    const THashMap<ui64, std::shared_ptr<TPortionInfo>>& GetPortions() const {
        return Portions;
    }

    const std::shared_ptr<TPortionInfo>& GetPortionVerified(const ui64 portionId) const {
        auto it = Portions.find(portionId);
        AFL_VERIFY(it != Portions.end());
        return it->second;
    }

    TExternalTasksContext(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& portions)
        : Portions(portions)
    {

    }
};

class TInternalTasksContext {
public:
};

}