#include "wait_for_tablets_state.h"

#include <mapreduce/yt/interface/client.h>

#include <util/generic/vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void WaitForTabletsState(const IClientPtr& client, const TYPath& table, ETabletState state, const TWaitForTabletsStateOptions& options)
{
    const TString stateString = ::ToString(state);
    yvector<TString> tabletStatePathList;
    const TInstant startTime = TInstant::Now();
    while (TInstant::Now() - startTime < options.Timeout_) {
        auto tabletList = client->Get(table + "/@tablets");
        bool good = true;
        for (const auto& tablet : tabletList.AsList()) {
            if (tablet["state"].AsString() != stateString) {
                good = false;
                break;
            }
        }
        if (good) {
            return;
        }

        Sleep(options.CheckInterval_);
    }
    ythrow yexception() << "WaitForTableState timeout";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
