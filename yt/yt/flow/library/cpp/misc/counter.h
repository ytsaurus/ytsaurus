#pragma once

#include <yt/yt/core/misc/ema_counter.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TSimpleEmaCounter
{
public:
    explicit TSimpleEmaCounter(TDuration window = TDuration::Seconds(30));

    void Update(double total, TInstant now = TInstant::Now());

    void Inc(double count = 1, TInstant now = TInstant::Now());

    std::optional<double> GetRate(TInstant now = TInstant::Now()) const;

    double GetTotal() const;

    void SetWindow(TDuration window);

private:
    double Total_ = 0;
    NYT::TEmaCounter<double, 1> Counter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
