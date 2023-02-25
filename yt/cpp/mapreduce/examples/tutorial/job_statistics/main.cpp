#include <yt/cpp/mapreduce/interface/client.h>
#include <util/system/user.h>

using namespace NYT;

class TComputeEmailsMapper
    : public IMapper<TTableReader<TNode>, TTableWriter<TNode>>
{
    public:
        void Do(TReader* reader, TWriter* writer) override
        {
            // Соберём несколько интересных статистик по сотрудникам
            i64 fortyTwoCount = 0;
            i64 shortLoginCount = 0;
            for (const auto& cursor : *reader) {
                const auto& row = cursor.GetRow();
                auto login = row["login"].AsString();
                TNode outRow;
                outRow["name"] = row["name"];
                outRow["email"] = login + "@yandex-team.ru";
                writer->AddRow(outRow);

                if (login.find("42") != TString::npos) {
                    ++fortyTwoCount;
                }
                if (login.size() <= 4) {
                    ++shortLoginCount;
                }
            }

            // Пользовательскую статистику можно писать по пути (имени)
            WriteCustomStatistics("names/with_42", fortyTwoCount);
            WriteCustomStatistics(TNode() // А можно в виде мапы
                ("names", TNode()
                    ("short", shortLoginCount)));
        }
};
REGISTER_MAPPER(TComputeEmailsMapper);

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");
    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-emails";

    auto operation = client->Map(
        TMapOperationSpec()
            .AddInput<TNode>("//home/dev/tutorial/staff_unsorted")
            .AddOutput<TNode>(outputTable),
        new TComputeEmailsMapper);

    auto jobStatistics = operation->GetJobStatistics();
    // Выведем системные статистики
    Cout << "time/total: " << jobStatistics.GetStatisticsAs<TDuration>("time/total").Sum() << Endl;
    Cout << "data/input/row_count: " << jobStatistics.GetStatistics("data/input/row_count").Sum() << Endl;

    // NB: для получения пользовательских статистик используется другой метод
    Cout << "names/with_42: " << jobStatistics.GetCustomStatistics("names/with_42").Sum() << Endl;
    Cout << "names/short: " << jobStatistics.GetCustomStatistics("names/short").Sum() << Endl;
    return 0;
}
