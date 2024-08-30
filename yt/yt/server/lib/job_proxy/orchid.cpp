#include "orchid.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TJobIOOrchidInfo::BuildOrchid(NYTree::TFluentAny fluent) const
{
    fluent.BeginMap()
        .Item("buffer_row_count").Value(BufferRowCount)
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TJobProxyOrchidInfo::BuildOrchid(TFluentAny fluent) const
{
    fluent.BeginMap()
        .Item("job_io").Do(std::bind(
            &TJobIOOrchidInfo::BuildOrchid,
            JobIOInfo,
            std::placeholders::_1))
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
