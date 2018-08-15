#pragma once

#include <yt/ytlib/formats/format.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/unversioned_row.h>

namespace NYT {
namespace NFormats {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TestNameTableExpansion(ISchemalessFormatWriterPtr writer, TNameTablePtr nameTable)
{
    // We write five rows, on each iteration we double number of 
    // columns in the NameTable. 
    for (int iteration = 0; iteration < 5; ++iteration) {
        TUnversionedOwningRowBuilder row;
        for (int index = 0; index < (1 << iteration); ++index) {
            auto key = "Column" + ToString(index);
            auto value = "Value" + ToString(index);
            int columnId = nameTable->GetIdOrRegisterName(key);
            row.AddValue(MakeUnversionedStringValue(value, columnId));
        }
        auto completeRow = row.FinishRow();
        EXPECT_EQ(true, writer->Write({completeRow.Get()}));
    }
    writer->Close()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
