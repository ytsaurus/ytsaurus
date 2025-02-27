#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

struct TLeaseManagerConfig
    : public NYTree::TYsonStruct
{
    TDuration LeaseRemovalPeriod;

    int MaxLeasesPerRemoval;

    REGISTER_YSON_STRUCT(TLeaseManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLeaseManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
