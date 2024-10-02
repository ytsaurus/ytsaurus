#pragma once

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<NYTree::TYsonStruct> TConfig>
bool AreConfigsEqual(const TIntrusivePtr<TConfig>& oldConfig, const TIntrusivePtr<TConfig>& newConfig)
{
    return NYTree::AreNodesEqual(
        NYTree::ConvertToNode(newConfig),
        NYTree::ConvertToNode(oldConfig));
}

////////////////////////////////////////////////////////////////////////////////

template <typename TOptional>
void InitIfNoValue(TOptional& dst, const typename TOptional::value_type& src)
{
    if (!dst) {
        dst = src;
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateDbName(const TString& dbName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
