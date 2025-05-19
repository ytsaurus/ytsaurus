#include "public.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

const std::string DefaultTabletCellBundleName("default");
const std::string SequoiaTabletCellBundleName("sequoia");

const TTimeFormula DefaultTabletBalancerSchedule = MakeTimeFormula("minutes % 5 == 0");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
