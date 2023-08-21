#include "incumbent_descriptor.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NIncumbentClient {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TIncumbentDescriptor& descriptor, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("addresses").Value(descriptor.Addresses)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TIncumbentMap CreateEmptyIncumbentMap()
{
    TIncumbentMap incumbentMap;
    for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
        auto& descriptor = incumbentMap[incumbentType];
        descriptor.Addresses.resize(GetIncumbentShardCount(incumbentType));
    }

    return incumbentMap;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentClient
