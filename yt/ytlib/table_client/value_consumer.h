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
    virtual void OnStringScalar(const TStringBuf& value, bool hasAttributes);
    virtual void OnIntegerScalar(i64 value, bool hasAttributes);
    virtual void OnDoubleScalar(double value, bool hasAttributes);
    virtual void OnEntity(bool hasAttributes);
    virtual void OnBeginList();
    virtual void OnBeginMap();

    bool NewValue;
    TKey* Key;
    int KeyIndex;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
