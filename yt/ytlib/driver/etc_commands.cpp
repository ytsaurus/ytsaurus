#include "etc_commands.h"

#include <ytlib/ypath/rich.h>

#include <ytlib/ytree/convert.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

void TParseYPathCommand::DoExecute()
{
    try {
        auto richPath = NYPath::TRichYPath::Parse(Request->Path);
        ReplySuccess(NYTree::ConvertToYsonString(richPath));
    } catch (const std::exception& ex) {
        ReplyError(ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
