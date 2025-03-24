#pragma once
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/library/accessor/accessor.h>
#include <contrib/ydb/services/metadata/request/config.h>

#include <util/datetime/base.h>

namespace NKikimr::NCSIndex {

class TConfig {
private:
    YDB_READONLY_DEF(NMetadata::NRequest::TConfig, RequestConfig);
    YDB_READONLY(TString, InternalTablePath, ".ext_index/tasks");
    YDB_READONLY_FLAG(Enabled, true);
public:
    bool DeserializeFromProto(const NKikimrConfig::TExternalIndexConfig& config);
    TString GetTablePath() const;
};
}
