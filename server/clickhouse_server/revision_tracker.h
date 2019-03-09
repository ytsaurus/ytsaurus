#pragma once

#include <yt/client/ypath/public.h>

#include <yt/client/api/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Class that allows tracking revision changes for an object in Cypress.
class TRevisionTracker
{
public:
    TRevisionTracker(NYPath::TYPath path, NApi::IClientPtr client);

    bool HasRevisionChanged() const;

    void FixCurrentRevision();

    std::optional<ui64> GetRevision() const;

private:
    NYPath::TYPath Path_;
    mutable NApi::IClientPtr Client_;
    std::optional<ui64> Revision_;

    std::optional<ui64> GetCurrentRevision() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
