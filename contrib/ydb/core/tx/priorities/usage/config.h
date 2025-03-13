#pragma once
#include <contrib/ydb/library/accessor/accessor.h>
#include <contrib/ydb/core/protos/config.pb.h>

namespace NKikimr::NPrioritiesQueue {

class TConfig {
private:
    YDB_READONLY(ui32, Limit, 32);
    YDB_READONLY_FLAG(Enabled, true);
public:
    bool DeserializeFromProto(const NKikimrConfig::TPrioritiesQueueConfig& config);
    TString DebugString() const;
};

}
