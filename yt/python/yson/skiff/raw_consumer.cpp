#include "raw_consumer.h"

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/finally.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

TPythonSkiffRawRecordBuilder::TPythonSkiffRawRecordBuilder(size_t schemasCount, TCallback<void()> endRowCallback)
    : SchemasCount_(schemasCount)
    , EndRowCallback_(endRowCallback)
{ }

void TPythonSkiffRawRecordBuilder::OnBeginRow(ui16 schemaIndex)
{
    if (schemaIndex >= SchemasCount_) {
        THROW_ERROR_EXCEPTION("Invalid schema index")
            << TErrorAttribute("schema_index", schemaIndex)
            << TErrorAttribute("schema_count", SchemasCount_);
    }
}

void TPythonSkiffRawRecordBuilder::OnEndRow()
{
    EndRowCallback_.Run();
}

void TPythonSkiffRawRecordBuilder::OnStringScalar(const TStringBuf& value, ui16 columnId)
{ }

void TPythonSkiffRawRecordBuilder::OnInt64Scalar(i64 value, ui16 columnId)
{ }

void TPythonSkiffRawRecordBuilder::OnUint64Scalar(ui64 value, ui16 columnId)
{ }

void TPythonSkiffRawRecordBuilder::OnDoubleScalar(double value, ui16 columnId)
{ }

void TPythonSkiffRawRecordBuilder::OnBooleanScalar(bool value, ui16 columnId)
{ }

void TPythonSkiffRawRecordBuilder::OnEntity(ui16 columnId)
{ }

void TPythonSkiffRawRecordBuilder::OnYsonString(const TStringBuf& value, ui16 columnId)
{ }

void TPythonSkiffRawRecordBuilder::OnOtherColumns(const TStringBuf& value)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
