#pragma once

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/client/table_client/schema.h>  //  NYT::NTableClient::TTableSchema

#include <util/ysaveload.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TSaveLoadableTableSchemaWrapper
{
    void Save(IOutputStream* output) const;
    void Load(IInputStream* input);

    NYT::NTableClient::TTableSchemaPtr Value;
};

TSaveLoadableTableSchemaWrapper& SaveLoadableTableSchema(NYT::NTableClient::TTableSchemaPtr& value);
const TSaveLoadableTableSchemaWrapper& SaveLoadableTableSchema(const NYT::NTableClient::TTableSchemaPtr& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

