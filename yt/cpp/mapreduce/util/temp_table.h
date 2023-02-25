#pragma once

#include <yt/cpp/mapreduce/interface/client.h>

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

    TString Name() const &;
    TString Name() && = delete;
    TString Release(); // Release table and return its name. Table will not be deleted by TTempTable after this call

private:
    IClientBasePtr Client_;
    TYPath Name_;
    bool Owns_ = true;

private:
    void RemoveTable();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
