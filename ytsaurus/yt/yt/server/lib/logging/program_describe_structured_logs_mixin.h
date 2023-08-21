#pragma once

#include <yt/yt/library/program/program.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TProgramDescribeStructuredLogsMixin
{
protected:
    explicit TProgramDescribeStructuredLogsMixin(NLastGetopt::TOpts& opts);

    bool HandleDescribeStructuredLogsOptions();

private:
    bool DescribeStructuredLogs_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
