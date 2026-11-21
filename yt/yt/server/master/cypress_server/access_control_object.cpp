#include "access_control_object.h"
#include "access_control_object_namespace.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

TAccessControlObject::TAccessControlObject(TAccessControlObjectId id)
    : TObject(id)
    , Acd_(this)
    , PrincipalAcd_(this)
{ }

void TAccessControlObject::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Name_);
    PHOENIX_REGISTER_FIELD(2, Namespace_);
    PHOENIX_REGISTER_FIELD(3, Acd_);
    PHOENIX_REGISTER_FIELD(4, PrincipalAcd_);
}

PHOENIX_DEFINE_TYPE(TAccessControlObject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
