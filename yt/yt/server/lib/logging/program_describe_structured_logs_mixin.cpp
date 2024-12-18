#include "category_registry.h"

#include "program_describe_structured_logs_mixin.h"

#include <yt/yt/core/yson/writer.h>

#include <library/cpp/yt/system/exit.h>

namespace NYT::NLogging {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TProgramDescribeStructuredLogsMixin::TProgramDescribeStructuredLogsMixin(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption(
        "describe-structured-logs",
        "Describes existing structured logs")
        .SetFlag(&DescribeStructuredLogsFlag_)
        .Optional();

    RegisterMixinCallback([&] { Handle(); });
}

void TProgramDescribeStructuredLogsMixin::Handle()
{
    if (DescribeStructuredLogsFlag_) {
        TYsonWriter writer(&Cout, EYsonFormat::Pretty);
        TStructuredCategoryRegistry::Get()->DumpCategories(&writer);
        Cout << Endl;
        Exit(EProcessExitCode::OK);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
