#pragma once

#include "public.h"

#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TValueConsumer
    : public NYTree::TYsonWriter
{
public:
    TValueConsumer(TOutputStream* output);
    void OnNewValue(TKey* key, int keyIndex);

private:
    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnIntegerScalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntity();
    virtual void OnBeginList();
    virtual void OnBeginMap();

    bool NewValue;
    TKey* Key;
    int KeyIndex;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
