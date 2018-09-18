#include "helpers.h"

#include <yt/ytlib/core_dump/proto/core_info.pb.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NCoreDump {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TCoreInfo& coreInfo, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("process_id").Value(coreInfo.process_id())
            .Item("executable_name").Value(coreInfo.executable_name())
            .DoIf(coreInfo.has_size(), [&] (TFluentMap fluent) {
                fluent
                    .Item("size").Value(coreInfo.size());
            })
            .DoIf(coreInfo.has_error(), [&] (TFluentMap fluent) {
                fluent
                    .Item("error").Value(NYT::FromProto<TError>(coreInfo.error()));
            })
        .EndMap();
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
