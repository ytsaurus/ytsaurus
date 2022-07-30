#pragma once

#include "public.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/journal_client/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

class TJournalHunkChunkWriterConfig
    : public NApi::TJournalChunkWriterConfig
{
public:
    i64 MaxRecordSize;
    i64 MaxRecordHunkCount;

    REGISTER_YSON_STRUCT(TJournalHunkChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalHunkChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TJournalHunkChunkWriterOptions
    : public NApi::TJournalChunkWriterOptions
{
    REGISTER_YSON_STRUCT(TJournalHunkChunkWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJournalHunkChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
