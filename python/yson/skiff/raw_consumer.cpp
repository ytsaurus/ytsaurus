#include "raw_consumer.h"

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/finally.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

TPythonSkiffRawRecordBuilder::TPythonSkiffRawRecordBuilder(size_t schemaCount, TCallback<void()> endRowCallback)
    : SchemaCount_(schemaCount)
    , EndRowCallback_(endRowCallback)
{ }

void TPythonSkiffRawRecordBuilder::OnBeginRow(ui16 schemaIndex)
{
    if (schemaIndex >= SchemaCount_) {
        THROW_ERROR_EXCEPTION("Invalid schema index")
            << TErrorAttribute("schema_index", schemaIndex)
            << TErrorAttribute("schema_count", SchemaCount_);
    }
}

void TPythonSkiffRawRecordBuilder::OnEndRow()
{
    EndRowCallback_.Run();
}

void TPythonSkiffRawRecordBuilder::OnStringScalar(TStringBuf /*value*/, ui16 /*columnId*/)
{ }

void TPythonSkiffRawRecordBuilder::OnInt64Scalar(i64 /*value*/, ui16 /*columnId*/)
{ }

void TPythonSkiffRawRecordBuilder::OnUint64Scalar(ui64 /*value*/, ui16 /*columnId*/)
{ }

void TPythonSkiffRawRecordBuilder::OnDoubleScalar(double /*value*/, ui16 /*columnId*/)
{ }

void TPythonSkiffRawRecordBuilder::OnBooleanScalar(bool /*value*/, ui16 /*columnId*/)
{ }

void TPythonSkiffRawRecordBuilder::OnEntity(ui16 /*columnId*/)
{ }

void TPythonSkiffRawRecordBuilder::OnYsonString(TStringBuf /*value*/, ui16 /*columnId*/)
{ }

void TPythonSkiffRawRecordBuilder::OnOtherColumns(TStringBuf /*value*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
