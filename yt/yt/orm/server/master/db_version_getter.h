#pragma once

#include <yt/yt/library/program/program.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TDBVersionGetter
{
protected:
    TDBVersionGetter(
        NLastGetopt::TOpts& opts,
        int dbVersion);

    bool HandleGetDBVersion() const;

private:
    const int DBVersion_;

    bool PrintDBVersion_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster

