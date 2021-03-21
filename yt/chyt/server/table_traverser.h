#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////


class TTableTraverser
{
using FilterByNameFunction = std::function<bool(const std::string &)>;

public:
    TTableTraverser(
        NApi::NNative::IClientPtr client,
        std::vector<TString> roots,
        const FilterByNameFunction& filterByTableName);

    const std::vector<std::string>& GetTables() const;

private:
    NApi::NNative::IClientPtr Client_;
    std::vector<TString> Roots_;
    std::vector<std::string> Tables_;
    const FilterByNameFunction& FilterByTableName_;

    void TraverseTablesFromRoots();
    void TraverseTablesFromNode(NYTree::INodePtr node);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
