#pragma once

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/client/table_client/logical_type.h>  // ::NYT::NTableClient::TLogicalTypePtr

#include <util/ysaveload.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TSaveLoadableLogicalTypeWrapper
{
    void Save(IOutputStream* output) const;
    void Load(IInputStream* input);

    ::NYT::NTableClient::TLogicalTypePtr Value;
};

TSaveLoadableLogicalTypeWrapper& SaveLoadableLogicalType(::NYT::NTableClient::TLogicalTypePtr& value);
const TSaveLoadableLogicalTypeWrapper& SaveLoadableLogicalType(const ::NYT::NTableClient::TLogicalTypePtr& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

