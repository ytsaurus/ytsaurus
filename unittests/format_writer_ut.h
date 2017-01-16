#include <yt/ytlib/formats/format.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NFormats {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TestNameTableExpansion(ISchemalessFormatWriterPtr writer, TNameTablePtr nameTable)
{
    // We write five rows, on each iteration we double number of 
    // columns in the NameTable. 
    for (int iteration = 0; iteration < 5; ++iteration) {
        TUnversionedRowBuilder row;
        for (int index = 0; index < (1 << iteration); ++index) {
            int columnId = nameTable->GetIdOrRegisterName("Column" + ToString(index));
            row.AddValue(MakeUnversionedStringValue("Value" + ToString(index), columnId));
        }
        EXPECT_EQ(true, writer->Write({ row.GetRow() }));
    }
    writer->Close()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
