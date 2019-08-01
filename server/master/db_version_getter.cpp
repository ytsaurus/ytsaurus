#include "db_version_getter.h"

#include <yp/server/objects/private.h>

namespace NYP::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

TDBVersionGetter::TDBVersionGetter(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("db-version", "print DB version and exit")
        .NoArgument()
        .SetFlag(&PrintDBVersion_);
}

bool TDBVersionGetter::HandleGetDBVersion() const
{
    if (PrintDBVersion_) {
        Cout << "DB version is " << NObjects::DBVersion << Endl;
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NMaster

