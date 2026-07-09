#include "graph_entity_id.h"

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
