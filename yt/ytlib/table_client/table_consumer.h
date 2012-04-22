#pragma once

#include "public.h"
#include "value_consumer.h"
#include "key.h"

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/misc/blob_output.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableConsumer
    : public NYTree::TForwardingYsonConsumer
{
public:
    TTableConsumer(const ISyncWriterPtr& writer);

private:
    void OnMyStringScalar(const TStringBuf& value);
    void OnMyIntegerScalar(i64 value);
    void OnMyDoubleScalar(double value);
    void OnMyEntity();
    void OnMyBeginList();
    void OnMyListItem();
    void OnMyBeginMap();
    void OnMyMapItem(const TStringBuf& name);
    void OnMyEndMap();

    // We currently ignore user attributes.
    // void OnMyBeginAttributes();

    void OnValueEnded();

    void OnColumn();

    ISyncWriterPtr Writer;
    TNullable<TKeyColumns> KeyColumns;

    bool InsideRow;

    size_t ValueOffset;
    TStringBuf CurrentColumn;

    TKey CurrentKey;

    yhash_set<TStringBuf> UsedColumns;

    TRow Row;
    TBlobOutput RowBuffer;
    TValueConsumer ValueConsumer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
