#include <yt/cpp/mapreduce/interface/client.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TFilterRobotsReduce
    : public IReducer<TTableReader<TNode>, TTableWriter<TNode>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        TNode loginRow;
        bool isRobot = false;
        for (auto& cursor : *reader) {
            const auto& curRow = cursor.GetRow();

            // У нас есть информация о том из какой таблицы пришла запись.
            auto tableIndex = cursor.GetTableIndex();
            if (tableIndex == 0) {
                // Таблица с логинами.
                loginRow = curRow;
            } else if (tableIndex == 1) {
                // Таблица про роботов.
                isRobot = curRow["is_robot"].AsBool();
            } else {
                // Какая-то фигня, такого индекса быть не может.
                Y_ABORT();
            }
        }

        if (isRobot) {
            writer->AddRow(loginRow);
        }
    }
};
REGISTER_REDUCER(TFilterRobotsReduce)

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    const TString sortedLoginTable = "//tmp/" + GetUsername() + "-tutorial-login-sorted";
    const TString sortedIsRobotTable = "//tmp/" + GetUsername() + "-tutorial-is_robot-sorted";
    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-robots";

    client->Sort(
        TSortOperationSpec()
            .AddInput("//home/tutorial/staff_unsorted")
            .Output(sortedLoginTable)
            .SortBy({"uid"}));

    client->Sort(
        TSortOperationSpec()
            .AddInput("//home/tutorial/is_robot_unsorted")
            .Output(sortedIsRobotTable)
            .SortBy({"uid"}));

    client->Reduce(
        TReduceOperationSpec()
            .ReduceBy({"uid"})
            .AddInput<TNode>(sortedLoginTable) // Таблицу с логинами мы добавляем первой, поэтому её TableIndex == 0
            .AddInput<TNode>(sortedIsRobotTable) // Таблицу про роботов мы добавляем второй, поэтому её TableIndex == 1
            .AddOutput<TNode>(outputTable),
        new TFilterRobotsReduce);

    Cout << "Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << outputTable << Endl;
    return 0;
}
