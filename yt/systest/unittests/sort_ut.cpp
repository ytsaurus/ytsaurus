
#include <yt/systest/sort_dataset.h>

#include <yt/systest/unittests/stub_dataset.h>
#include <yt/systest/unittests/util.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTest {

class TSortTest : public ::testing::Test
{
};

TEST_F(TSortTest, Basic)
{
    TTable table{{
        TDataColumn{
            "a",
            NProto::EColumnType::ELatinString100,
            std::nullopt
        },
        TDataColumn{
            "b",
            NProto::EColumnType::ELatinString100,
            std::nullopt
        },
        TDataColumn{
            "c",
            NProto::EColumnType::EInt16,
            std::nullopt
        }
    },
    {}  // DeletedColumnNames
    };

    std::vector<std::vector<TNode>> data{
        {"x", "x", 10},
        {"x", "y", 15},
        {"x", "x", 10},
        {"z", "a", 12},
        {"z", "b", 15},
        {"z", "a", 10},
    };

    TStubDataset dataset(table, data);

    TSortOperation sortOperation{
        {"c"}
    };

    TSortDataset sortDataset(dataset, sortOperation);

    auto iterator = sortDataset.NewIterator();

    std::vector<std::vector<TNode>> expectedSorted{
        {10, "x", "x"},
        {10, "x", "x"},
        {10, "z", "a"},
        {12, "z", "a"},
        {15, "x", "y"},
        {15, "z", "b"},
    };

    ExpectEqual(expectedSorted, iterator.get());
}

}  // namespace NYT::NTest
