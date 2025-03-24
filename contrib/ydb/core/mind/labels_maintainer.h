#pragma once
#include "defs.h"

#include <contrib/ydb/core/protos/config.pb.h>

namespace NKikimr {

IActor *CreateLabelsMaintainer(const NKikimrConfig::TMonitoringConfig &config);

} // namespace NKikimr
