#include <yt/core/test_framework/framework.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_sorted_merging_reader.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TResultStorage
{
public:
    void OnUnreadRows(TRange<TUnversionedRow> unreadRows)
    {
        UnreadRowCount_ = unreadRows.Size();
        if (!unreadRows.Empty()) {
            FirstUnreadRow_ = ToString(unreadRows[0]);
        }
    }

    int GetUnreadRowCount() const
    {
        return UnreadRowCount_;
    }

    const TString& GetFirstUnreadRow() const
    {
        return FirstUnreadRow_;
    }

private:
    i64 UnreadRowCount_;
    TString FirstUnreadRow_;
};

////////////////////////////////////////////////////////////////////////////////

struct TTableData
{
    TString Schema;
    std::vector<TString> Rows;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkFakeReader
    : public virtual IReaderBase
    , public ISchemalessMultiChunkReader
{
public:
    TSchemalessMultiChunkFakeReader(const TTableData& tableData, int inputTableIndex, TResultStorage* resultStorage = nullptr)
        : TableData_(tableData)
        , InputTableIndex_(inputTableIndex)
        , ResultStorage_(resultStorage)
    {
        TableSchema_ = ConvertTo<TTableSchema>(TYsonString(tableData.Schema));
        NameTable_ = New<TNameTable>();
        for (int i = 0; i < TableSchema_.GetColumnCount(); ++i) {
            NameTable_->RegisterName(TableSchema_.Columns()[i].Name());
        }
        NameTable_->RegisterName(TableIndexColumnName);
    }

    virtual TFuture<void> GetReadyEvent()
    {
        return VoidFuture;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const
    {
        Y_UNREACHABLE();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        Y_UNIMPLEMENTED();
    }

    virtual bool IsFetchingCompleted() const
    {
        return true;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const
    {
        Y_UNREACHABLE();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows)
    {
        Rows_.clear();
        rows->clear();
        if (Interrupted_ || RowIndex_ >= TableData_.Rows.size()) {
            return false;
        }
        TString tableIndexYson = Format("; \"@table_index\"=%d", InputTableIndex_);
        while (rows->size() < rows->capacity() && RowIndex_ < TableData_.Rows.size()) {
            Rows_.emplace_back(YsonToSchemafulRow(TableData_.Rows[RowIndex_] + tableIndexYson, TableSchema_, false));
            rows->emplace_back(Rows_.back());
            ++RowIndex_;
        }
        return true;
    }

    virtual const TNameTablePtr& GetNameTable() const
    {
        return NameTable_;
    }

    virtual TKeyColumns GetKeyColumns() const
    {
        return TableSchema_.GetKeyColumns();
    }

    virtual i64 GetTableRowIndex() const
    {
        return RowIndex_;
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(
        NYT::TRange<TUnversionedRow> unreadRows) const
    {
        ResultStorage_->OnUnreadRows(unreadRows);
        return {};
    }

    virtual i64 GetSessionRowIndex() const
    {
        return RowIndex_;
    }

    virtual i64 GetTotalRowCount() const
    {
        return TableData_.Rows.size();
    }

    virtual void Interrupt()
    {
        Interrupted_ = true;
    }

private:
    const TTableData& TableData_;
    int InputTableIndex_ = 0;
    TResultStorage* ResultStorage_ = nullptr;

    TTableSchema TableSchema_;
    TNameTablePtr NameTable_;
    int RowIndex_ = 0;
    bool Interrupted_ = false;
    std::vector<TUnversionedOwningRow> Rows_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessSortedMergingReaderTest
    : public ::testing::Test
{
protected:
    typedef std::function<ISchemalessMultiChunkReaderPtr(std::vector<TResultStorage>* resultStorage)> TReaderFactory;

    void ReadAndCheckResult(
        TReaderFactory createReader,
        std::vector<TResultStorage> *resultStorage,
        int rowsPerRead,
        int interruptRowCount,
        int expectedReadRowCount,
        TString expectedLastReadRow,
        std::vector<std::pair<int,TString>> expectedResult)
    {
        auto reader = createReader(resultStorage);
        std::vector<TUnversionedRow> rows;
        rows.reserve(rowsPerRead);
        int readRowCount = 0;
        TString lastReadRow;
        bool interrupted = false;
        if (readRowCount >= interruptRowCount && !interrupted) {
            reader->Interrupt();
            interrupted = true;
        }
        WaitFor(reader->GetReadyEvent());
        while (reader->Read(&rows)) {
            if (!rows.empty()) {
                lastReadRow = ToString(rows.back());
                readRowCount += rows.size();
            }
            if (readRowCount >= interruptRowCount && !interrupted) {
                reader->Interrupt();
                interrupted = true;
            }
            WaitFor(reader->GetReadyEvent());
        }
        reader->GetInterruptDescriptor(NYT::TRange<TUnversionedRow>());
        EXPECT_EQ(readRowCount, expectedReadRowCount);
        EXPECT_EQ(lastReadRow, expectedLastReadRow);
        for (int primaryTableId = 0; primaryTableId < static_cast<int>(resultStorage->size()); ++primaryTableId) {
            EXPECT_EQ((*resultStorage)[primaryTableId].GetUnreadRowCount(), expectedResult[primaryTableId].first);
            if ((*resultStorage)[primaryTableId].GetUnreadRowCount() != 0) {
                EXPECT_EQ((*resultStorage)[primaryTableId].GetFirstUnreadRow(), expectedResult[primaryTableId].second);
            }
        }
    }

    std::vector<TString> ReadAll(
        TReaderFactory createReader,
        std::vector<TResultStorage> *resultStorage)
    {
        std::vector<TString> result;
        auto reader = createReader(resultStorage);
        std::vector<TUnversionedRow> rows;
        rows.reserve(1);
        WaitFor(reader->GetReadyEvent());
        while (reader->Read(&rows)) {
            for (const auto& row : rows) {
                result.emplace_back(ToString(row));
            }
            WaitFor(reader->GetReadyEvent());
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

const TTableData tableData0 {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending};"
        "{name = c1; type = int64; sort_order = ascending};"
        "{name = c2; type = uint64; sort_order = ascending}; ]",
    {
        "c0=ab; c1=1; c2=21u",
        "c0=ab; c1=1; c2=22u",
        "c0=bb; c1=2; c2=23u",
        "c0=bb; c1=2; c2=24u",
        "c0=cb; c1=3; c2=25u",
        "c0=cb; c1=3; c2=26u",
    }
};

const TTableData tableData1 {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending};"
        "{name = c1; type = int64; sort_order = ascending};"
        "{name = c2; type = uint64; sort_order = ascending}; ]",
    {
        "c0=aa; c1=1; c2=1u",
        "c0=ab; c1=3; c2=3u",
        "c0=ac; c1=5; c2=5u",
        "c0=ba; c1=7; c2=7u",
        "c0=bb; c1=9; c2=9u",
        "c0=bc; c1=11; c2=11u",
        "c0=ca; c1=13; c2=13u",
        "c0=cb; c1=15; c2=15u",
        "c0=cc; c1=17; c2=17u",
    }
};

const TTableData tableData2 {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending};"
        "{name = c1; type = int64};"
        "{name = c2; type = uint64}; ]",
    {
        "c0=aa; c1=2; c2=2u",
        "c0=ab; c1=4; c2=4u",
        "c0=ac; c1=6; c2=6u",
        "c0=ba; c1=8; c2=8u",
        "c0=bb; c1=10; c2=10u",
        "c0=bc; c1=12; c2=12u",
        "c0=ca; c1=14; c2=14u",
        "c0=cb; c1=16; c2=16u",
        "c0=cc; c1=18; c2=18u",
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessSortedMergingReaderTest, SortedMergingReaderSingleTable)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 0, &(*resultStorage)[0]));

        return CreateSchemalessSortedMergingReader(primaryReaders, 2, 2);
    };

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);
    for (int interruptRowCount = 0; interruptRowCount < static_cast<int>(rows.size()); ++interruptRowCount) {
        int rowsPerRead = 1;
        ReadAndCheckResult(
            createReader,
            &resultStorage,
            rowsPerRead,
            interruptRowCount,
            interruptRowCount,
            interruptRowCount != 0 ? rows[interruptRowCount - 1] : TString(""),
            {
                {0, TString("")},
            });
    }
}

TEST_F(TSchemalessSortedMergingReaderTest, SortedMergingReaderMultipleTables)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(2);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 0, &(*resultStorage)[0]));
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 1, &(*resultStorage)[1]));

        return CreateSchemalessSortedMergingReader(primaryReaders, 3, 2);
    };

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 4;
    int rowsPerRead = 1;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        interruptRowCount,
        rows[interruptRowCount - 1],
        {
            {7, rows[4]},
            {7, rows[5]},
        });

    interruptRowCount = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        interruptRowCount,
        rows[interruptRowCount - 1],
        {
            {6, rows[6]},
            {7, rows[5]},
        });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessSortedMergingReaderTest, SortedJoiningReaderForeignBeforeMultiplePrimary)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(2);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 1, &(*resultStorage)[0]));
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 2, &(*resultStorage)[1]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 0));

        return CreateSchemalessSortedJoiningReader(primaryReaders, 3, 2, foreignReaders, 1);
    };

    // Expected sequence of rows:
    // ["aa", 2, 2u, 0]
    // ["aa", 1, 1u, 2]
    // ["ab", 4, 4u, 0]
    // ["ab", 1, 21u, 1]
    // ["ab", 1, 22u, 1]
    // ["ab", 3, 3u, 2]
    // ["ac", 6, 6u, 0]
    // ["ac", 5, 5u, 2]
    // ["ba", 8, 8u, 0]
    // ["ba", 7, 7u, 2]
    // ["bb", 10, 10u, 0]
    // ["bb", 2, 23u, 1]
    // ["bb", 2, 24u, 1]
    // ["bb", 9, 9u, 2]
    // ["bc", 12, 12u, 0]
    // ["bc", 11, 11u, 2]
    // ["ca", 14, 14u, 0]
    // ["ca", 13, 13u, 2]
    // ["cb", 16, 16u, 0]
    // ["cb", 3, 25u, 1]
    // ["cb", 3, 26u, 1]
    // ["cb", 15, 15u, 2]
    // ["cc", 18, 18u, 0]
    // ["cc", 17, 17u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        3,
        rows[2],
        {
            {6, TString("[0#\"ab\", 1#1, 2#21u, 3#1]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#2]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[4],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#1]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#2]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[4],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#1]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#2]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#1]")},
            {7, TString("[0#\"ac\", 1#5, 2#5u, 3#2]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        7,
        rows[6],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#1]")},
            {7, TString("[0#\"ac\", 1#5, 2#5u, 3#2]")},
        });
}

TEST_F(TSchemalessSortedMergingReaderTest, SortedJoiningReaderMultiplePrimaryBeforeForeign)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(2);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 0, &(*resultStorage)[0]));
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 1, &(*resultStorage)[1]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 2));

        return CreateSchemalessSortedJoiningReader(primaryReaders, 3, 2, foreignReaders, 1);
    };

    // Expected sequence of rows:
    // ["aa", 1, 1u, 1]
    // ["aa", 2, 2u, 2]
    // ["ab", 1, 21u, 0]
    // ["ab", 1, 22u, 0]
    // ["ab", 3, 3u, 1]
    // ["ab", 4, 4u, 2]
    // ["ac", 5, 5u, 1]
    // ["ac", 6, 6u, 2]
    // ["ba", 7, 7u, 1]
    // ["ba", 8, 8u, 2]
    // ["bb", 2, 23u, 0]
    // ["bb", 2, 24u, 0]
    // ["bb", 9, 9u, 1]
    // ["bb", 10, 10u, 2]
    // ["bc", 11, 11u, 1]
    // ["bc", 12, 12u, 2]
    // ["ca", 13, 13u, 1]
    // ["ca", 14, 14u, 2]
    // ["cb", 3, 25u, 0]
    // ["cb", 3, 26u, 0]
    // ["cb", 15, 15u, 1]
    // ["cb", 16, 16u, 2]
    // ["cc", 17, 17u, 1]
    // ["cc", 18, 18u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#1]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {8, TString("[0#\"ab\", 1#3, 2#3u, 3#1]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {7, TString("[0#\"ac\", 1#5, 2#5u, 3#1]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {7, TString("[0#\"ac\", 1#5, 2#5u, 3#1]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
            {6, TString("[0#\"ba\", 1#7, 2#7u, 3#1]")},
        });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessSortedMergingReaderTest, SortedJoiningReaderMultipleForeignBeforePrimary)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 2, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 0));
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 1));

        return CreateSchemalessSortedJoiningReader(primaryReaders, 3, 2, foreignReaders, 1);
    };

    // Expected sequence of rows:
    // ["ab", 3, 3u, 0]
    // ["ab", 4, 4u, 1]
    // ["ab", 1, 21u, 2]
    // ["ab", 1, 22u, 2]
    // ["bb", 9, 9u, 0]
    // ["bb", 10, 10u, 1]
    // ["bb", 2, 23u, 2]
    // ["bb", 2, 24u, 2]
    // ["cb", 15, 15u, 0]
    // ["cb", 16, 16u, 1]
    // ["cb", 3, 25u, 2]
    // ["cb", 3, 26u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[4],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#2]")},
        });
}

TEST_F(TSchemalessSortedMergingReaderTest, SortedJoiningReaderPrimaryBeforeMultipleForeign)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 0, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 1));
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 2));

        return CreateSchemalessSortedJoiningReader(primaryReaders, 3, 2, foreignReaders, 1);
    };

    // Expected sequence of rows:
    // ["ab", 1, 21u, 0]
    // ["ab", 1, 22u, 0]
    // ["ab", 3, 3u, 1]
    // ["ab", 4, 4u, 2]
    // ["bb", 2, 23u, 0]
    // ["bb", 2, 24u, 0]
    // ["bb", 9, 9u, 1]
    // ["bb", 10, 10u, 2]
    // ["cb", 3, 25u, 0]
    // ["cb", 3, 26u, 0]
    // ["cb", 15, 15u, 1]
    // ["cb", 16, 16u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessSortedMergingReaderTest, JoinReduceJoiningReaderForeignBeforePrimary)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 2, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 0));
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 1));

        return CreateSchemalessJoinReduceJoiningReader(primaryReaders, 1, 1, foreignReaders, 1);
    };

    // Expected sequence of rows:
    // ["ab", 3, 3u, 0]
    // ["ab", 4, 4u, 1]
    // ["ab", 1, 21u, 2]
    // ["ab", 1, 22u, 2]
    // ["bb", 9, 9u, 0]
    // ["bb", 10, 10u, 1]
    // ["bb", 2, 23u, 2]
    // ["bb", 2, 24u, 2]
    // ["cb", 15, 15u, 0]
    // ["cb", 16, 16u, 1]
    // ["cb", 3, 25u, 2]
    // ["cb", 3, 26u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        3,
        rows[2],
        {
            {5, TString("[0#\"ab\", 1#1, 2#22u, 3#2]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        5,
        rows[4],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        6,
        rows[5],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#2]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        7,
        rows[6],
        {
            {3, TString("[0#\"bb\", 1#2, 2#24u, 3#2]")},
        });
}

TEST_F(TSchemalessSortedMergingReaderTest, JoinReduceJoiningReaderPrimaryBeforeForeign)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData0, 0, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData1, 1));
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData2, 2));

        return CreateSchemalessJoinReduceJoiningReader(primaryReaders, 1, 1, foreignReaders, 1);
    };

    // Expected sequence of rows:
    // ["ab", 1, 21u, 0]
    // ["ab", 1, 22u, 0]
    // ["ab", 3, 3u, 1]
    // ["ab", 4, 4u, 2]
    // ["bb", 2, 23u, 0]
    // ["bb", 2, 24u, 0]
    // ["bb", 9, 9u, 1]
    // ["bb", 10, 10u, 2]
    // ["cb", 3, 25u, 0]
    // ["cb", 3, 26u, 0]
    // ["cb", 15, 15u, 1]
    // ["cb", 16, 16u, 2]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 3;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
        });
    interruptRowCount = 4;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        4,
        rows[3],
        {
            {4, TString("[0#\"bb\", 1#2, 2#23u, 3#0]")},
        });
    interruptRowCount = 5;
    rowsPerRead = 5;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        7,
        rows[7], // Note: rows[6] should be skipped
        {
            {3, TString("[0#\"bb\", 1#2, 2#24u, 3#0]")},
        });
    interruptRowCount = 6;
    rowsPerRead = 2;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
    interruptRowCount = 7;
    rowsPerRead = 7;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {2, TString("[0#\"cb\", 1#3, 2#25u, 3#0]")},
        });
}

////////////////////////////////////////////////////////////////////////////////

const TTableData tableData3 {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending}; ]",
    {
        "c0=a; c1=3",
        "c0=a; c1=3",
        "c0=a; c1=3",
        "c0=b; c1=3",
        "c0=b; c1=3",
        "c0=b; c1=3",
    }
};

const TTableData tableData4 {
    "<strict=%false>["
        "{name = c0; type = string; sort_order = ascending}; ]",
    {
        "c0=a; c1=4",
        "c0=b; c1=4",
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemalessSortedMergingReaderTest, JoinReduceJoiningReaderCheckLastRows)
{
    auto createReader = [] (std::vector<TResultStorage>* resultStorage) -> ISchemalessMultiChunkReaderPtr {
        resultStorage->clear();
        resultStorage->resize(1);
        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        primaryReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData4, 1, &(*resultStorage)[0]));

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        foreignReaders.emplace_back(New<TSchemalessMultiChunkFakeReader>(tableData3, 0));

        return CreateSchemalessJoinReduceJoiningReader(primaryReaders, 1, 1, foreignReaders, 1);
    };

    // Expected sequence of rows:
    // ["a", 3, 0]
    // ["a", 3, 0]
    // ["a", 3, 0]
    // ["a", 4, 1]
    // ["b", 3, 0]
    // ["b", 3, 0]
    // ["b", 3, 0]
    // ["b", 4, 1]

    std::vector<TResultStorage> resultStorage;
    auto rows = ReadAll(createReader, &resultStorage);

    int interruptRowCount = 8;
    int rowsPerRead = 3;
    ReadAndCheckResult(
        createReader,
        &resultStorage,
        rowsPerRead,
        interruptRowCount,
        8,
        rows[7],
        {
            {0, TString("")},
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
