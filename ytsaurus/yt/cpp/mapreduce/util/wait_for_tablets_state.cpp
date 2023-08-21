#include "wait_for_tablets_state.h"

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void WaitForTabletsState(const IClientPtr& client, const TYPath& table, ETabletState state, const TWaitForTabletsStateOptions& options)
{
    const TString stateString = ::ToString(state);
    const TInstant startTime = TInstant::Now();
    while (TInstant::Now() - startTime < options.Timeout_) {
        auto tabletState = client->Get(table + "/@tablet_state").AsString();
        if (tabletState == stateString) {
            return;
        }

        Sleep(options.CheckInterval_);
    }
    ythrow yexception() << "WaitForTableState timeout";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
