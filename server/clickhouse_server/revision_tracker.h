#pragma once

#include <yt/client/ypath/public.h>

#include <yt/client/api/public.h>

#include <yt/client/hydra/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Class that allows tracking revision changes for an object in Cypress.
class TRevisionTracker
{
public:
    TRevisionTracker(NYPath::TYPath path, NApi::IClientPtr client);

    bool HasRevisionChanged() const;

    void FixCurrentRevision();

    NHydra::TRevision GetRevision() const;

private:
    const NYPath::TYPath Path_;
    const NApi::IClientPtr Client_;

    NHydra::TRevision Revision_;

    NHydra::TRevision GetCurrentRevision() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
