#include "format_hints.h"

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/interface/operation.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <>
void ApplyFormatHints<TNode>(TFormat* format, const TMaybe<TFormatHints>& formatHints)
{
    Y_VERIFY(format);
    if (!formatHints) {
        return;
    }

    if (formatHints->SkipNullValuesForTNode_) {
        Y_ENSURE_EX(
            format->Config.AsString() == "yson",
            TApiUsageError() << "SkippNullForTNode option must be used with yson format, actual format: " << format->Config.AsString());
        format->Config.Attributes()["skip_null_values"] = formatHints->SkipNullValuesForTNode_;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
