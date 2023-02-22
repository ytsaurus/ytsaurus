#include "temp_table.h"

#include <mapreduce/yt/interface/config.h>

#include <util/system/yassert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TTempTable::TTempTable(
    IClientBasePtr client,
    const TString& prefix,
    const TYPath& path,
    const TCreateOptions& options)
    : Client_(client)
{
    if (path) {
        if (!options.Recursive_ && !Client_->Exists(path)) {
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

    Client_->Create(Name_, NT_TABLE, options);
}

TTempTable::TTempTable(TTempTable&& sourceTable)
    : Client_(sourceTable.Client_)
    , Name_(sourceTable.Name_)
    , Owns_(sourceTable.Owns_)
{
    sourceTable.Owns_ = false;
}

TTempTable& TTempTable::operator=(TTempTable&& sourceTable)
{
    if (&sourceTable == this) {
        return *this;
    }

    if (Owns_) {
        RemoveTable();
    }

    Client_ = sourceTable.Client_;
    Name_ = sourceTable.Name_;
    Owns_ = sourceTable.Owns_;

    sourceTable.Owns_ = false;

    return *this;
}

void TTempTable::RemoveTable()
{
    Client_->Remove(Name_, TRemoveOptions().Force(true));
}

TTempTable::~TTempTable()
{
    if (Owns_) {
        try {
            RemoveTable();
        } catch (...) {
        }
    }
}

TString TTempTable::Name() const &
{
    return Name_;
}

TString TTempTable::Release()
{
    Y_ASSERT(Owns_);
    Owns_ = false;
    return Name_;
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
