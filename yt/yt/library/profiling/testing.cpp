#include "testing.h"
#include "impl.h"

#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

double TTesting::ReadGauge(const TGauge& gauge)
{
    if (gauge.Gauge_) {
        return gauge.Gauge_->GetValue();
    }

    ythrow yexception() << "Gauge is not registered";
}

i64 TTesting::ReadCounter(const TCounter& counter)
{
    if (counter.Counter_) {
        return counter.Counter_->GetValue();
    }

    ythrow yexception() << "Counter is not registered";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
