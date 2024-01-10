#include <yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_protobuf/data.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

class TSplitHumanRobotsReduce
    // Обратите внимание наш редьюс работает с несколькими типами записей
    // как на вход так и на выход, поэтому мы используем ::google::protobuf::Message
    : public IReducer<
        TTableReader<::google::protobuf::Message>,
        TTableWriter<::google::protobuf::Message>>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        TUserRecord userRecord;
        bool isRobot = false;
        for (auto& cursor : *reader) {
            auto tableIndex = cursor.GetTableIndex();
            // Мы знаем номер таблицы и поэтому мы можем запросить конкретный тип protobuf'а в этом месте.
            // Тип protobuf сообщения передаётся шаблонным аргументом к методу `GetRow()'.
            if (tableIndex == 0) {
                userRecord = cursor.GetRow<TUserRecord>();
            } else if (tableIndex == 1) {
                const auto& isRobotRecord = cursor.GetRow<TIsRobotRecord>();
                isRobot = isRobotRecord.GetIsRobot();
            } else {
                Y_ABORT();
            }
        }

        // В AddRow мы можем передавать как TRobotRecord так и THumanRecord.
        if (isRobot) {
            TRobotRecord robotRecord;
            robotRecord.SetUid(userRecord.GetUid());
            robotRecord.SetLogin(userRecord.GetLogin());
            writer->AddRow(robotRecord, 0);
        } else {
            THumanRecord humanRecord;
            humanRecord.SetName(userRecord.GetName());
            humanRecord.SetLogin(userRecord.GetLogin());
            humanRecord.SetEmail(userRecord.GetLogin() + "@yandex-team.ru");
            writer->AddRow(humanRecord, 1);
        }
    }
};
REGISTER_REDUCER(TSplitHumanRobotsReduce)

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    const TString sortedUserTable = "//tmp/" + GetUsername() + "-tutorial-user-sorted";
    const TString sortedIsRobotTable = "//tmp/" + GetUsername() + "-tutorial-is_robot-sorted";
    const TString humanTable = "//tmp/" + GetUsername() + "-tutorial-humans";
    const TString robotTable = "//tmp/" + GetUsername() + "-tutorial-robots";

    client->Sort(
        TSortOperationSpec()
            .AddInput("//home/tutorial/staff_unsorted")
            .Output(sortedUserTable)
            .SortBy({"uid"}));

    client->Sort(
        TSortOperationSpec()
            .AddInput("//home/tutorial/is_robot_unsorted")
            .Output(sortedIsRobotTable)
            .SortBy({"uid"}));

    client->Reduce(
        TReduceOperationSpec()
            .ReduceBy({"uid"})
            .AddInput<TUserRecord>(sortedUserTable)
            .AddInput<TIsRobotRecord>(sortedIsRobotTable)
            .AddOutput<TRobotRecord>(robotTable)
            .AddOutput<THumanRecord>(humanTable),
        new TSplitHumanRobotsReduce);

    Cout << "Robot table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << robotTable << Endl;
    Cout << "Human table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << humanTable << Endl;

    return 0;
}
