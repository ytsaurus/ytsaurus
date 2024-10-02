#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TBatchSizeBackoff
{
public:
    TBatchSizeBackoff(const NLogging::TLogger& logger, TBatchSizeBackoffConfigPtr config, i64 initialBatchSize);

    i64 operator*() const;

    void Next();
    void Rollback();
    void RestrictLimit(i64 value);

private:
    const NLogging::TLogger& Logger;
    const TBatchSizeBackoffConfigPtr Config_;

    i64 Current_;
    i64 Additive_;
    i64 Limit_;
    bool IncreasingExponentially_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
