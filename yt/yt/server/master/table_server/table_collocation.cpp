#include "table_collocation.h"

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

std::string TTableCollocation::GetLowercaseObjectName() const
{
    return Format("table collocation %v", GetId());
}

std::string TTableCollocation::GetCapitalizedObjectName() const
{
    return Format("Table collocation %v", GetId());
}

void TTableCollocation::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, ExternalCellTag_);
    PHOENIX_REGISTER_FIELD(2, Tables_);
    PHOENIX_REGISTER_FIELD(3, Type_);
    PHOENIX_REGISTER_FIELD(4, ReplicationCollocationOptions_,
        .template Serializer<TDerefSerializer<>>());
}

PHOENIX_DEFINE_TYPE(TTableCollocation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
