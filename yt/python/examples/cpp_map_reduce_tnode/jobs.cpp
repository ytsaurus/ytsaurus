#include <yt/cpp/mapreduce/interface/client.h>

#include <util/charset/utf8.h>

using namespace NYT;

class TNormalizeNameMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            TNode outRow;
            outRow["name"] = ToLowerUTF8(row["name"].AsString());

            writer->AddRow(outRow);
        }
    }
};
REGISTER_MAPPER(TNormalizeNameMapper);

class TCountNamesReducer
    : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        int count = 0;
        TString name;
        for (auto& cursor : *reader) {
            name = cursor.GetRow()["name"].AsString();
            ++count;
        }
        TNode outRow;
        outRow["name"] = name;
        outRow["count"] = count;

        writer->AddRow(outRow);
    }
};
REGISTER_REDUCER(TCountNamesReducer);
