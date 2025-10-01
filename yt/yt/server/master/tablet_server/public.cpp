#include "public.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

const std::string DefaultTabletCellBundleName("default");
const std::string SequoiaChunksTabletCellBundleName("sequoia-chunks");
const std::string SequoiaCypressTabletCellBundleName("sequoia-cypress");

const TTimeFormula DefaultTabletBalancerSchedule = MakeTimeFormula("minutes % 5 == 0");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
