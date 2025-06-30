#include "save_loadable_tableschema_wrapper.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TSaveLoadableTableSchemaWrapper::Save(IOutputStream* output) const
{
    bool hasValue = static_cast<bool>(Value);
    ::Save(output, hasValue);
    if (hasValue) {
        NYT::TStreamSaveContext context(output);
        Value->Save(context);
    }
}

void TSaveLoadableTableSchemaWrapper::Load(IInputStream* input)
{
    bool hasValue = false;
    ::Load(input, hasValue);
    if (hasValue) {
        Value = NYT::New<NYT::NTableClient::TTableSchema>();
        NYT::TStreamLoadContext context(input);
        Value->Load(context);
    } else {
        Value = {};
    }
}

TSaveLoadableTableSchemaWrapper& SaveLoadableTableSchema(NYT::NTableClient::TTableSchemaPtr& value)
{
    return *reinterpret_cast<TSaveLoadableTableSchemaWrapper*>(&value);
}

const TSaveLoadableTableSchemaWrapper& SaveLoadableTableSchema(const NYT::NTableClient::TTableSchemaPtr& value)
{
    return *reinterpret_cast<const TSaveLoadableTableSchemaWrapper*>(&value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

