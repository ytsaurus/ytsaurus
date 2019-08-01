#pragma once

#include <yt/ytlib/program/program.h>

namespace NYP::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TDBVersionGetter
{
protected:
    TDBVersionGetter(NLastGetopt::TOpts& opts);

    bool HandleGetDBVersion() const;

private:
    bool PrintDBVersion_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NMaster

