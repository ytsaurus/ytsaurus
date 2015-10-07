#include "stdafx.h"
#include "framework.h"

#include <ytlib/table_client/unversioned_row.h>
#include <ytlib/table_client/name_table.h>

#include <ytlib/formats/yamr_writer.h>

#include <core/concurrency/async_stream.h>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

TEST(TSchemalessYamrWriter, Simple)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto valueId = nameTable->RegisterName("value");
    
    auto config = New<TYamrFormatConfig>();
    
    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", keyId));
    row1.AddValue(MakeUnversionedStringValue("value1", valueId));
    
    // Note that key and value follow not in order.
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("value2", valueId));
    row2.AddValue(MakeUnversionedStringValue("key2", keyId));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();

    Stroka output =
        "key1\tvalue1\n"
        "key2\tvalue2\n";
    
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriterTest, SimpleWithSubkey)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto subkeyId = nameTable->RegisterName("subkey");
    auto valueId = nameTable->RegisterName("value");

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    
    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", keyId));
    row1.AddValue(MakeUnversionedStringValue("value1", valueId));
    row1.AddValue(MakeUnversionedStringValue("subkey1", subkeyId));
    
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("subkey2", subkeyId));
    row2.AddValue(MakeUnversionedStringValue("value2", valueId));
    row2.AddValue(MakeUnversionedStringValue("key2", keyId));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();

    Stroka output =
        "key1\tsubkey1\tvalue1\n"
        "key2\tsubkey2\tvalue2\n";
    
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriterTest, SubkeyCouldBeSkipped)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto subkeyId = nameTable->RegisterName("subkey");
    auto valueId = nameTable->RegisterName("value");

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("key", keyId));
    row.AddValue(MakeUnversionedStringValue("value", valueId));
    
    std::vector<TUnversionedRow> rows = { row.GetRow() };
    
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    (void)subkeyId; // To suppress warning about `subkeyId` being unused.

    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();

    Stroka output = "key\t\tvalue\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriterTest, SkippedKey)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto valueId = nameTable->RegisterName("value");

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("value", valueId));
    (void)keyId; // To suppress warning about `keyId` being unused.

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    auto config = New<TYamrFormatConfig>();
    
    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    EXPECT_FALSE(writer->Write(rows));
}

TEST(TSchemalessYamrWriterTest, SkippedValue)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto valueId = nameTable->RegisterName("value");

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("key", keyId));
    (void)valueId; // To suppress warning about `valueId` being unused.

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    auto config = New<TYamrFormatConfig>();
    
    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    EXPECT_FALSE(writer->Write(rows));
}

TEST(TSchemalessYamrWriterTest, ExtraItem)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto valueId = nameTable->RegisterName("value");
    auto trashId = nameTable->RegisterName("trash");

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("key", keyId));
    row.AddValue(MakeUnversionedStringValue("value", valueId));
    row.AddValue(MakeUnversionedStringValue("trash", trashId));

    std::vector<TUnversionedRow> rows = { row.GetRow() };

    auto config = New<TYamrFormatConfig>();

    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable,
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving
        false, // enableKeySwitch
        0, // keyColumnCount
        config);
    
    EXPECT_FALSE(writer->Write(rows));
}

TEST(TSchemalessYamrWriterTest, Escaping)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto subkeyId = nameTable->RegisterName("subkey");
    auto valueId = nameTable->RegisterName("value");

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->EnableEscaping = true;
    
    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedStringValue("\n", keyId));
    row.AddValue(MakeUnversionedStringValue("\t", subkeyId));
    row.AddValue(MakeUnversionedStringValue("\n", valueId));
   
    std::vector<TUnversionedRow> rows = { row.GetRow() };

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();

    Stroka output = "\\n\t\\t\t\\n\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriter, SimpleWithTableIndex)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto valueId = nameTable->RegisterName("value");
    
    auto config = New<TYamrFormatConfig>();
    config->EnableTableIndex = true;
    
    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    writer->WriteTableIndex(42);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", keyId));
    row1.AddValue(MakeUnversionedStringValue("value1", valueId));
    
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", keyId));
    row2.AddValue(MakeUnversionedStringValue("value2", valueId));
    
    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };
    EXPECT_EQ(true, writer->Write(rows));
   
    writer->WriteTableIndex(23);

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", keyId));
    row3.AddValue(MakeUnversionedStringValue("value3", valueId));

    rows = { row3.GetRow() };
    EXPECT_EQ(true, writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    Stroka output =
        "42\n"
        "key1\tvalue1\n"
        "key2\tvalue2\n"
        "23\n"
        "key3\tvalue3\n";
    
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriter, Lenval)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto subkeyId = nameTable->RegisterName("subkey");
    auto valueId = nameTable->RegisterName("value");

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    // Note that order in both rows is unusual.
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("value1", valueId));
    row1.AddValue(MakeUnversionedStringValue("key1", keyId));
    row1.AddValue(MakeUnversionedStringValue("subkey1", subkeyId));
    
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", keyId));
    row2.AddValue(MakeUnversionedStringValue("value2", valueId));
    row2.AddValue(MakeUnversionedStringValue("subkey2", subkeyId));

    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();
    
    Stroka output = Stroka(
        "\x04\x00\x00\x00" "key1"
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x07\x00\x00\x00" "subkey2"
        "\x06\x00\x00\x00" "value2"
        , 2 * (3 * 4 + 4 + 6 + 7) // all i32 + lengths of keys
    );
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriterTest, LenvalWithEmptyFields)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto subkeyId = nameTable->RegisterName("subkey");
    auto valueId = nameTable->RegisterName("value");

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);
    
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("", keyId));
    row1.AddValue(MakeUnversionedStringValue("subkey1", subkeyId));
    row1.AddValue(MakeUnversionedStringValue("value1", valueId));
    
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", keyId));
    row2.AddValue(MakeUnversionedStringValue("", subkeyId));
    row2.AddValue(MakeUnversionedStringValue("value2", valueId));
    
    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", keyId));
    row3.AddValue(MakeUnversionedStringValue("subkey3", subkeyId));
    row3.AddValue(MakeUnversionedStringValue("", valueId));
    
    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow(), row3.GetRow() };

    EXPECT_EQ(true, writer->Write(rows));
    writer->Close()
        .Get()
        .ThrowOnError();
    
    Stroka output = Stroka(
        "\x00\x00\x00\x00" ""
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x00\x00\x00\x00" ""
        "\x06\x00\x00\x00" "value2"

        "\x04\x00\x00\x00" "key3"
        "\x07\x00\x00\x00" "subkey3"
        "\x00\x00\x00\x00" ""

        , 9 * 4 + (7 + 6) + (4 + 6) + (4 + 7) // all i32 + lengths of keys
    );

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriterTest, LenvalWithKeySwitch)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto subkeyId = nameTable->RegisterName("subkey");
    auto valueId = nameTable->RegisterName("value");

    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;

    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        true, // enableKeySwitch
        1, // keyColumnCount
        config);
    
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", keyId));
    row1.AddValue(MakeUnversionedStringValue("subkey1", subkeyId));
    row1.AddValue(MakeUnversionedStringValue("value1", valueId));
    
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", keyId));
    row2.AddValue(MakeUnversionedStringValue("subkey21", subkeyId));
    row2.AddValue(MakeUnversionedStringValue("value21", valueId));
    
    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key2", keyId));
    row3.AddValue(MakeUnversionedStringValue("subkey22", subkeyId));
    row3.AddValue(MakeUnversionedStringValue("value22", valueId));
    
    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow(), row3.GetRow() };
    EXPECT_EQ(true, writer->Write(rows));
    
    TUnversionedRowBuilder row4;
    row4.AddValue(MakeUnversionedStringValue("key3", keyId));
    row4.AddValue(MakeUnversionedStringValue("subkey3", subkeyId));
    row4.AddValue(MakeUnversionedStringValue("value3", valueId));
    
    rows = { row4.GetRow() };
    EXPECT_EQ(true, writer->Write(rows));
    
    writer->Close()
        .Get()
        .ThrowOnError();
    
    Stroka output = Stroka(
        "\x04\x00\x00\x00" "key1"
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"
        
        "\xfe\xff\xff\xff" // key switch

        "\x04\x00\x00\x00" "key2"
        "\x08\x00\x00\x00" "subkey21"
        "\x07\x00\x00\x00" "value21"

        "\x04\x00\x00\x00" "key2"
        "\x08\x00\x00\x00" "subkey22"
        "\x07\x00\x00\x00" "value22"

        "\xfe\xff\xff\xff"

        "\x04\x00\x00\x00" "key3"
        "\x07\x00\x00\x00" "subkey3"
        "\x06\x00\x00\x00" "value3"

        , 14 * 4 + (4 + 7 + 6) + (4 + 8 + 7) + (4 + 8 + 7) + (4 + 7 + 6) // all i32 + lengths of keys
    );

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriter, LenvalWithTableIndex)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto valueId = nameTable->RegisterName("value");
    
    auto config = New<TYamrFormatConfig>();
    config->EnableTableIndex = true;
    config->Lenval = true;

    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    writer->WriteTableIndex(42);

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", keyId));
    row1.AddValue(MakeUnversionedStringValue("value1", valueId));
    
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", keyId));
    row2.AddValue(MakeUnversionedStringValue("value2", valueId));
    
    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };
    EXPECT_EQ(true, writer->Write(rows));
   
    writer->WriteTableIndex(23);

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", keyId));
    row3.AddValue(MakeUnversionedStringValue("value3", valueId));

    rows = { row3.GetRow() };
    EXPECT_EQ(true, writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    Stroka output(
        "\xff\xff\xff\xff" "\x2a\x00\x00\x00" // 42
        
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"
        
        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"
        
        "\xff\xff\xff\xff" "\x17\x00\x00\x00" // 23
        
        "\x04\x00\x00\x00" "key3"
        "\x06\x00\x00\x00" "value3"
    , 10 * 4 + 3 * (4 + 6));

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TSchemalessYamrWriter, LenvalWithRangeAndRowIndex)
{
    auto nameTable = New<TNameTable>();
    auto keyId = nameTable->RegisterName("key");
    auto valueId = nameTable->RegisterName("value");
    
    auto config = New<TYamrFormatConfig>();
    config->Lenval = true;

    TStringStream outputStream;
    auto writer = New<TSchemalessYamrWriter>(
        nameTable, 
        CreateAsyncAdapter(static_cast<TOutputStream*>(&outputStream)),
        false, // enableContextSaving  
        false, // enableKeySwitch
        0, // keyColumnCount
        config);

    writer->WriteRangeIndex(static_cast<i32>(42));
    
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("key1", keyId));
    row1.AddValue(MakeUnversionedStringValue("value1", valueId));
    
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("key2", keyId));
    row2.AddValue(MakeUnversionedStringValue("value2", valueId));
    
    std::vector<TUnversionedRow> rows = { row1.GetRow(), row2.GetRow() };
    EXPECT_EQ(true, writer->Write(rows));
   
    writer->WriteRowIndex(static_cast<i64>(23));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("key3", keyId));
    row3.AddValue(MakeUnversionedStringValue("value3", valueId));

    rows = { row3.GetRow() };
    EXPECT_EQ(true, writer->Write(rows));

    writer->Close()
        .Get()
        .ThrowOnError();

    Stroka output(
        "\xfd\xff\xff\xff" "\x2a\x00\x00\x00" // 42
        
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"
        
        "\x04\x00\x00\x00" "key2"
        "\x06\x00\x00\x00" "value2"
        
        "\xfc\xff\xff\xff" "\x17\x00\x00\x00\x00\x00\x00\x00" // 23
        
        "\x04\x00\x00\x00" "key3"
        "\x06\x00\x00\x00" "value3"
    , 11 * 4 + 3 * (4 + 6));

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
