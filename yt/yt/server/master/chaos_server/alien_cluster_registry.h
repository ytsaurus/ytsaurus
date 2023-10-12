#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TAlienClusterRegistry
    : public TRefCounted
{
public:
    int GetOrRegisterAlienClusterIndex(const TString& clusterName);
    const TString& GetAlienClusterName(int alienClusterIndex) const;

    void Clear();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const std::vector<TString>& GetIndexToName() const;
    void Reset(std::vector<TString> indexToName);

private:
    THashMap<TString, int> NameToIndex_;
    std::vector<TString> IndexToName_;
};

DEFINE_REFCOUNTED_TYPE(TAlienClusterRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
