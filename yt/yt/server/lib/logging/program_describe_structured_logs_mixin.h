#pragma once

#include <yt/yt/library/program/program_mixin.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TProgramDescribeStructuredLogsMixin
    : public virtual TProgramMixinBase
{
protected:
    explicit TProgramDescribeStructuredLogsMixin(NLastGetopt::TOpts& opts);

private:
    bool DescribeStructuredLogs_ = false;

    void Handle();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
