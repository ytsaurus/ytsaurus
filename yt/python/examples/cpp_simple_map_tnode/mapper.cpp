#include <yt/cpp/mapreduce/interface/client.h>

using namespace NYT;

class TComputeEmailsMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>> // Указываем, что мы хотим использовать TNode
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        for (auto& cursor : *reader) {
            const auto& row = cursor.GetRow();

            TNode outRow;
            outRow["name"] = row["name"];
            outRow["email"] = row["login"].AsString() + "@yandex-team.ru";

            writer->AddRow(outRow);
        }
    }
};
REGISTER_MAPPER(TComputeEmailsMapper);
