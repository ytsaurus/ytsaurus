#include "category_registry.h"

#include "program_describe_structured_logs_mixin.h"

#include <yt/yt/core/yson/writer.h>

namespace NYT::NLogging {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TProgramDescribeStructuredLogsMixin::TProgramDescribeStructuredLogsMixin(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("describe-structured-logs", "describe existing structured logs")
        .StoreTrue(&DescribeStructuredLogs_)
        .Optional();
}

bool TProgramDescribeStructuredLogsMixin::HandleDescribeStructuredLogsOptions()
{
    if (DescribeStructuredLogs_) {
        TYsonWriter writer(&Cout, EYsonFormat::Pretty);
        TStructuredCategoryRegistry::Get()->DumpCategories(&writer);
        Cout << Endl;

        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
