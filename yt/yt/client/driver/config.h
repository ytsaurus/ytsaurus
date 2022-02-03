#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

constexpr int ApiVersion3 = 3;
constexpr int ApiVersion4 = 4;

class TDriverConfig
    : public NYTree::TYsonSerializable
{
public:
    NApi::TFileReaderConfigPtr FileReader;
    NApi::TFileWriterConfigPtr FileWriter;
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NApi::TJournalReaderConfigPtr JournalReader;
    NApi::TJournalWriterConfigPtr JournalWriter;
    NChunkClient::TFetcherConfigPtr Fetcher;
    int ApiVersion;

    i64 ReadBufferRowCount;
    i64 ReadBufferSize;
    i64 WriteBufferSize;

    TSlruCacheConfigPtr ClientCache;

    std::optional<TString> Token;

    TAsyncExpiringCacheConfigPtr ProxyDiscoveryCache;

    TDriverConfig();
};

DEFINE_REFCOUNTED_TYPE(TDriverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

