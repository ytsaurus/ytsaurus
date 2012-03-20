#include "stdafx.h"

#include "yson_table_input.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////

TYsonTableInput::TYsonTableInput(
    ISyncReader* reader, 
    NYTree::EYsonFormat format,
    TOutputStream* outputStream)
    : Reader(reader)
    , YsonWriter(outputStream, format)
{
    Reader->Open();
}

bool TYsonTableInput::ReadRow()
{
    Reader->NextRow();

    if (!Reader->IsValid())
        return false;

    YsonWriter.OnListItem();
    YsonWriter.OnBeginMap();
    FOREACH(auto& pair, Reader->GetRow()) {
        YsonWriter.OnMapItem(pair.first);
        YsonWriter.OnStringScalar(pair.second.ToString(), false);
    }
    YsonWriter.OnEndMap(false);
    return true;
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
