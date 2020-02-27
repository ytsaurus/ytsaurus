#pragma once

#include "fair_share_tree_element.h"
#include "fair_share_tree_element_classic.h"

#include <yt/core/logging/log.h>

#define OPERATION_LOG_DETAILED(operationElement, ...) \
    do { \
        const auto& Logger = operationElement->GetLogger(); \
        if (operationElement->DetailedLogsEnabled()) { \
            YT_LOG_DEBUG(__VA_ARGS__); \
        } else { \
            YT_LOG_TRACE(__VA_ARGS__); \
        } \
    } while(false)


