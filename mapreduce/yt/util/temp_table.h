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

    ~TTempTable();

    TString Name() const;

private:
    IClientBasePtr Client_;
    TYPath Name_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
