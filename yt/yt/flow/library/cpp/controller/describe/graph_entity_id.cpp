#include "graph_entity_id.h"

#include <library/cpp/yt/string/format.h>

#include <util/string/join.h>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

TGraphEntityId MakeStreamGraphId(const TStreamId& globalStreamId)
{
    return TGraphEntityId(Concat<std::string>(StreamPrefix, globalStreamId.Underlying()));
}

TGraphEntityId MakeSourceGraphId(const TStreamId& globalStreamId)
{
    return TGraphEntityId(Concat<std::string>(SourcePrefix, globalStreamId.Underlying()));
}

TGraphEntityId MakeSinkGraphId(const TSinkId& globalSinkId)
{
    return TGraphEntityId(Concat<std::string>(SinkPrefix, globalSinkId.Underlying()));
}

TGraphEntityId MakeComputationGraphId(const TComputationId& computationId)
{
    return TGraphEntityId(Concat<std::string>(ComputationPrefix, computationId.Underlying()));
}

TSinkId MakeGlobalSinkId(const TComputationId& computationId, const TSinkId& localSinkId)
{
    return TSinkId(Format("%v/%v", computationId, localSinkId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
