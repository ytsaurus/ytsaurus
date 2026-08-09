#pragma once

#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TMaterializedViewCoordinator
    : public TRefCounted
{
public:
    TMaterializedViewCoordinator(
        THost* host,
        TCypressObjectRepositoryPtr repository,
        TMaterializedViewsConfigPtr config);
    ~TMaterializedViewCoordinator();

    void Start();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TMaterializedViewCoordinator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
