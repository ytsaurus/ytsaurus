#pragma once

#include <yql/essentials/utils/log/proto/logger_config.pb.h>

namespace NYql {
    void ExtLoggingBackends(const NProto::TLoggingConfig& config);
} // namespace NYql
