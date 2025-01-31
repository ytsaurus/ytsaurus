#pragma once

#include <yt/cpp/mapreduce/interface/client.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief RAII class for working with temporary tables. Constructor of this class creates temporary table and destructor removes it.
///
/// CAVEAT: when using with transactions destructor of TTempTable should be called before commiting transaction.
class TTempTable
{
public:
    ///
    /// @brief Constructor creates temporary table
    ///
    /// @param client -- YT client or transaction object.
    /// @param prefix -- table name prefix
    /// @param directory -- path to directory where temporary table will be created.
    /// @param options -- options to be passed for table creation (might be useful if we want to set table attributes)
    explicit TTempTable(
        IClientBasePtr client,
        const TString& prefix = {},
        const TYPath& directory = {},
        const TCreateOptions& options = {});

    TTempTable(const TTempTable&) = delete;
    TTempTable& operator=(const TTempTable&) = delete;

    TTempTable(TTempTable&&);
    TTempTable& operator=(TTempTable&&);

    ~TTempTable();

    static TTempTable CreateAutoremovingTable(IClientBasePtr client, TYPath path, const TCreateOptions& options);

    /// Return full path to the table.
    TString Name() const &;
    TString Name() && = delete;

    /// Release table and return its path. Table will not be deleted by TTempTable destructor after this call.
    TString Release();

private:
    struct TPrivateConstuctorTag
    { };
    TTempTable(TPrivateConstuctorTag, IClientBasePtr client, TYPath path, const TCreateOptions& options);

private:
    IClientBasePtr Client_;
    TYPath Name_;
    bool Owns_ = true;

private:
    void RemoveTable();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
