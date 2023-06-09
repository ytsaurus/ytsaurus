#pragma once

#include "private.h"

#include <yt/yt/ytlib/hive/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

struct TYqlRowset
{
    TError Error;
    TSharedRef WireRowset;
    bool Incomplete = false;
};

std::vector<TYqlRowset> BuildRowsets(
    const NHiveClient::TClientDirectoryPtr& clientDirectory,
    const TString& yqlYsonResults,
    i64 rowCountLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
