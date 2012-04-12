#include "stdafx.h"
#include "yson_table_input.h"

#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////

TYsonTableInput::TYsonTableInput(
    ISyncTableReader* reader, 
    NYTree::EYsonFormat format,
    TOutputStream* outputStream)
    : Reader(reader)
    , YsonWriter(outputStream, format)
{
    Reader->Open();
}

bool TYsonTableInput::ReadRow()
{
    if (!Reader->IsValid())
        return false;

    YsonWriter.OnListItem();
    YsonWriter.OnBeginMap();
    FOREACH(auto& pair, Reader->GetRow()) {
        YsonWriter.OnMapItem(pair.first);
        YsonWriter.OnStringScalar(pair.second.ToString());
    }
    YsonWriter.OnEndMap();

    Reader->NextRow();
    return true;
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
