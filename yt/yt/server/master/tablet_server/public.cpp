#include "public.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultTabletCellBundleName("default");
const TString SequoiaTabletCellBundleName("sequoia");

const TTimeFormula DefaultTabletBalancerSchedule = MakeTimeFormula("minutes % 5 == 0");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
