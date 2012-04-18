#pragma once

#include "sync_writer.h"

#include <ytlib/ytree/yson_consumer.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TRowConsumer
    : public NYTree::TYsonConsumerBase
{
public:
    TRowConsumer(ISyncTableWriter* writer);

private:
    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnIntegerScalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntity();
    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList();
    virtual void OnBeginMap();
    virtual void OnKeyedItem(const TStringBuf& name);
    virtual void OnEndMap();
    virtual void OnBeginAttributes();
    virtual void OnEndAttributes();
    void CheckInsideRow();

    ISyncTableWriter* Writer;
    int RowIndex;
    bool InsideRow;
    TColumn Column;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
