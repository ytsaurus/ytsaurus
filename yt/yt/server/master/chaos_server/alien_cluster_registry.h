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
    int GetOrRegisterAlienClusterIndex(const std::string& clusterName);
    const std::string& GetAlienClusterName(int alienClusterIndex) const;

    void Clear();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const std::vector<std::string>& GetIndexToName() const;
    void Reset(std::vector<std::string> indexToName);

private:
    THashMap<std::string, int> NameToIndex_;
    std::vector<std::string> IndexToName_;
};

DEFINE_REFCOUNTED_TYPE(TAlienClusterRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
