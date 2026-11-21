#include "area.h"
#include "cell_bundle.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/lib/chaos_server/config.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string TArea::GetLowercaseObjectName() const
{
    return Format("area %Qv of bundle %Qv", GetName(), CellBundle_->GetName());
}

std::string TArea::GetCapitalizedObjectName() const
{
    return Format("Area %Qv of Bundle %Qv", GetName(), CellBundle_->GetName());
}

TYPath TArea::GetObjectPath() const
{
    return Format("//sys/areas/%v", NYPath::ToYPathLiteral(GetName()));
}

void TArea::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, CellBundle_);
    PHOENIX_REGISTER_FIELD(3, NodeTagFilter_);
    PHOENIX_REGISTER_FIELD(4, ChaosOptions_,
        .template Serializer<TNullableIntrusivePtrSerializer<>>());
}

PHOENIX_DEFINE_TYPE(TArea);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
