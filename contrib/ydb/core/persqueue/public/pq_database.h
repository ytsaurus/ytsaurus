#pragma once

#include <contrib/ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

    TString GetDatabaseFromConfig(const NKikimrPQ::TPQConfig& config);

} // namespace NKikimr::NPQ
