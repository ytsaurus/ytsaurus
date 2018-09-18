#include "public.h"

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultTabletCellBundleName("default");

const TTimeFormula DefaultTabletBalancerSchedule = MakeTimeFormula("minutes % 5 == 0");

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
