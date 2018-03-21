#pragma once

#include "record.h"
#include "public.h"
#include "schema.h"

#include <yt/python/common/helpers.h>
#include <yt/python/common/stream.h>

#include <Extensions.hxx> // pycxx
#include <Objects.hxx> // pycxx

#include <Python.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TPythonSkiffRawRecordBuilder
{
public:
    TPythonSkiffRawRecordBuilder(size_t schemasCount, TCallback<void()> endRowCallback);

    void OnBeginRow(ui16 schemaIndex);
    void OnEndRow();
    void OnStringScalar(const TStringBuf& value, ui16 columnId);
    void OnInt64Scalar(i64 value, ui16 columnId);
    void OnUint64Scalar(ui64 value, ui16 columnId);
    void OnDoubleScalar(double value, ui16 columnId);
    void OnBooleanScalar(bool value, ui16 columnId);
    void OnEntity(ui16 columnId);
    void OnYsonString(const TStringBuf& value, ui16 columnId);

    void OnOtherColumns(const TStringBuf& value);

private:
    size_t SchemasCount_;
    TCallback<void()> EndRowCallback_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
