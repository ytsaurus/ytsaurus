#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/misc/config.h>
#include <yt/yt/server/lib/misc/format_manager.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NServer {
namespace {

using namespace NFormats;
using namespace NScheduler;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TFormatManagerTest, ValidatesVanillaTaskWithYPathSpecialCharacters)
{
    auto formatConfig = New<TFormatConfig>();
    formatConfig->Enable = false;

    THashMap<EFormatType, TFormatConfigPtr> formatConfigs;
    formatConfigs.emplace(EFormatType::Yson, std::move(formatConfig));

    TFormatManager formatManager(std::move(formatConfigs), "test_user");
    auto spec = ConvertTo<INodePtr>(TYsonStringBuf(R"({
        tasks = {
            "process: //path/to/table" = {
                format = yson;
            };
        };
    })"));

    EXPECT_THROW_WITH_SUBSTRING(
        formatManager.ValidateAndPatchOperationSpec(spec, EOperationType::Vanilla),
        "is disabled");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NServer
