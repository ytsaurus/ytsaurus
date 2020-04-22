#pragma once

#include "public.h"

#include <yt/server/node/cell_node/public.h>

#include <yt/core/logging/log.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TDiskLocation
    : public TRefCounted
{
public:
    TDiskLocation(
        TDiskLocationConfigPtr config,
        const TString& id,
        const NLogging::TLogger& logger);

    //! Returns the string id.
    const TString& GetId() const;

    //! Returns |true| iff the location is enabled.
    bool IsEnabled() const;

protected:
    const TString Id_;

    mutable NLogging::TLogger Logger;
    std::atomic<bool> Enabled_ = false;

    void ValidateMinimumSpace() const;
    void ValidateLockFile() const;

    i64 GetTotalSpace() const;

private:
    const TDiskLocationConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

