#include <yt/cpp/mapreduce/interface/client.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TCountNamesReduce
    : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        // В Do приходят все записи с общим reduce ключом, т.е. в нашем случае с общим полем `name'.
        const auto& row = reader->GetRow();
        const auto name = row["name"];

        ui32 count = 0;
        for ([[maybe_unused]] auto& cursor : *reader) {
            ++count;
        }

        TNode result;
        result["name"] = name;
        result["count"] = count;
        writer->AddRow(result);
    }
};
REGISTER_REDUCER(TCountNamesReduce);

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    const TString sortedTmpTable = "//tmp/" + GetUsername() + "-tutorial-tmp";
    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-emails";

    client->Sort(
        TSortOperationSpec()
            .AddInput("//home/dev/tutorial/staff_unsorted")
            .Output(sortedTmpTable)
            .SortBy({"name"}));

    client->Reduce(
        TReduceOperationSpec()
            .ReduceBy({"name"})
            .AddInput<TNode>(sortedTmpTable) // Входная таблица должна быть отсортирована по тем колонкам,
                                             // по которым мы осуществляем reduce.
            .AddOutput<TNode>(outputTable),
        new TCountNamesReduce);

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
