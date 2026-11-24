#pragma once

#include <ydb-cpp-sdk/library/time/time.h>

#include <functional>

namespace NYdb::inline V3 {

class IClientImplCommon {
public:
    virtual ~IClientImplCommon() = default;
    virtual void ScheduleTask(const std::function<void()>& fn, TDeadline::Duration timeout) = 0;
};

}
