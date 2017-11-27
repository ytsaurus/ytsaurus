#pragma once

#include <mapreduce/yt/interface/client.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TTempTable
{
public:
    explicit TTempTable(
        IClientBasePtr client,
        const TString& prefix = {},
        const TYPath& path = {},
        const TCreateOptions& options = {});

    TTempTable(const TTempTable&) = delete;
    TTempTable& operator=(const TTempTable&) = delete;

    TTempTable(TTempTable&&);
    TTempTable& operator=(TTempTable&&);

    ~TTempTable();

    TString Name() const;

private:
    IClientBasePtr Client_;
    TYPath Name_;
    bool Owns_ = true;

private:
    void RemoveTable();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
