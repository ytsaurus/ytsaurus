#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <optional>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TAlienClusterRegistry
    : public TRefCounted
{
public:
    int GetOrRegisterAlienClusterIndex(const std::string& clusterName);
    std::optional<int> FindAlienClusterIndex(const std::string& clusterName) const;
    const std::string& GetAlienClusterName(int alienClusterIndex) const;

    void Clear();

    const std::vector<std::string>& GetIndexToName() const;
    void SetIndexToName(std::vector<std::string> indexToName);

private:
    THashMap<std::string, int> NameToIndex_;
    std::vector<std::string> IndexToName_;

    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TAlienClusterRegistry, 0x86e42af5);
};

DEFINE_REFCOUNTED_TYPE(TAlienClusterRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
