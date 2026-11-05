#pragma once

#include "column_meta.h"

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TBoundaryKeysExtension
{
    TProtobufString Min;
    TProtobufString Max;

    bool operator==(const TBoundaryKeysExtension& other) const = default;
};

struct TColumnMetaExtension
{
    std::vector<NTableChunkFormat::TColumnMeta> Columns;

    bool operator==(const TColumnMetaExtension& other) const = default;
};

struct TKeyColumnsExtension
{
    std::vector<std::string> Names;

    bool operator==(const TKeyColumnsExtension& other) const = default;
};

struct TSamplesExtension
{
    std::vector<std::string> Entries;
    std::vector<i32> Weights;

    bool operator==(const TSamplesExtension& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
