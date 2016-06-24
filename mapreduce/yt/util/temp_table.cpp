#include "temp_table.h"

#include <mapreduce/yt/common/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTempTable::TTempTable(
    IClientBasePtr client,
    const Stroka& prefix,
    const TYPath& path)
    : Client_(client)
{
    if (path) {
        if (!Client_->Exists(path)) {
            ythrow yexception() << "Path " << path << " does not exist";
        }
        Name_ = path;

    } else {
        Name_ = TConfig::Get()->RemoteTempTablesDirectory;
        Client_->Create(Name_, NT_MAP,
            TCreateOptions().IgnoreExisting(true).Recursive(true));
    }

    Name_ += "/";
    Name_ += prefix;
    Name_ += CreateGuidAsString();

    Client_->Create(Name_, NT_TABLE);
}

TTempTable::~TTempTable()
{
    try {
        Client_->Remove(Name_, TRemoveOptions().Force(true));
    } catch (...) {
    }
}

Stroka TTempTable::Name() const
{
    return Name_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
