#include "db_version_getter.h"

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

TDBVersionGetter::TDBVersionGetter(
    NLastGetopt::TOpts& opts,
    int dbVersion)
    : DBVersion_(dbVersion)
{
    opts.AddLongOption("db-version", "print DB version and exit")
        .NoArgument()
        .SetFlag(&PrintDBVersion_);
}

bool TDBVersionGetter::HandleGetDBVersion() const
{
    if (PrintDBVersion_) {
        Cout << "DB version is " << DBVersion_ << Endl;
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster

