#include "state.h"

#include <yt/yt/flow/lib/serializer/state.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

void TInternalState::Register(TRegistrar registrar)
{
    registrar.Parameter("state", &TThis::State)
        .AddOption(NYsonSerializer::EYsonStateValueType::Simple)
        .Default();
    registrar.Parameter("compressed", &TThis::Compressed)
        .AddOption(NYsonSerializer::EYsonStateValueType::Packable)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
