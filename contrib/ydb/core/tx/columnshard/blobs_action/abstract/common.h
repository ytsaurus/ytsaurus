#pragma once
#include <contrib/ydb/library/accessor/accessor.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/string.h>
#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>

namespace NKikimr::NOlap {

class ICommonBlobsAction {
private:
    YDB_READONLY_DEF(TString, StorageId);
    const i64 ActionId = 0;
public:
    i64 GetActionId() const {
        return ActionId;
    }

    ICommonBlobsAction(const TString& storageId);
    virtual ~ICommonBlobsAction() = default;
};

}
