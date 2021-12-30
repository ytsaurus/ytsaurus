#pragma once

#include "public.h"

#include <variant>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

class TYPathStack
{
public:
    void Push(TString key);
    void Push(int index);
    void IncreaseLastIndex();
    void Pop();
    bool IsEmpty() const;
    TYPath GetPath() const;
    TYPath GetHumanReadablePath() const;
    std::optional<TString> TryGetStringifiedLastPathToken() const;

private:
    using TEntry = std::variant<
        TString,
        int>;

    std::vector<TEntry> Items_;

    static TString ToString(const TEntry& entry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
