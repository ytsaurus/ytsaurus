#pragma once

#include "record.h"
#include "public.h"
#include "schema.h"

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/stream.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

#include <Python.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TPythonSkiffRawRecordBuilder
{
public:
    TPythonSkiffRawRecordBuilder(size_t schemaCount, TCallback<void()> endRowCallback);

    void OnBeginRow(ui16 schemaIndex);
    void OnEndRow();
    void OnStringScalar(TStringBuf value, ui16 columnId);
    void OnInt64Scalar(i64 value, ui16 columnId);
    void OnUint64Scalar(ui64 value, ui16 columnId);
    void OnDoubleScalar(double value, ui16 columnId);
    void OnBooleanScalar(bool value, ui16 columnId);
    void OnEntity(ui16 columnId);
    void OnYsonString(TStringBuf value, ui16 columnId);

    void OnOtherColumns(TStringBuf value);

private:
    const size_t SchemaCount_;
    const TCallback<void()> EndRowCallback_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
