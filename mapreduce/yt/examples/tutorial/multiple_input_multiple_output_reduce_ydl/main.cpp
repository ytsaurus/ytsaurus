#include <mapreduce/yt/examples/tutorial/multiple_input_multiple_output_reduce_ydl/data.ydl.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/config.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

namespace NData = mapreduce::yt::examples::tutorial::multiple_input_multiple_output_reduce_ydl::data;

class TSplitHumanRobotsReduce
    // Если ридер работает с таблицами, строки которых имеют один и тот же тип, можно просто передавать этот тип
    // шаблонным параметром в ридер, иначе нужно использовать TYdlOneOf, внутри которого перечисляются
    // все различные типы строк, которые встречаются во входных таблицах.
    : public IReducer<
          TTableReader<TYdlOneOf<NData::TUserRecord, NData::TIsRobotRecord>>,
          TYdlTableWriter>
{
public:
    void Do(TReader* reader, TWriter* writer) override {
        NData::TUserRecord userRecord;
        bool isRobot = false;
        for (; reader->IsValid(); reader->Next()) {
            auto tableIndex = reader->GetTableIndex();
            if (tableIndex == 0) {
                userRecord = reader->GetRow<NData::TUserRecord>();
            } else if (tableIndex == 1) {
                const auto& isRobotRecord = reader->GetRow<NData::TIsRobotRecord>();
                isRobot = isRobotRecord.GetIsRobot();
            } else {
                Y_FAIL();
            }
        }

        if (isRobot) {
            NData::TRobotRecord robotRecord;
            robotRecord.SetUid(userRecord.GetUid());
            robotRecord.SetLogin(userRecord.GetLogin());
            writer->AddRow(robotRecord, 0);
        } else {
            NData::THumanRecord humanRecord;
            humanRecord.SetName(userRecord.GetName());
            humanRecord.SetLogin(userRecord.GetLogin());
            humanRecord.SetEmail(userRecord.GetLogin() + "@yandex-team.ru");
            writer->AddRow(humanRecord, 1);
        }
    }
};
REGISTER_REDUCER(TSplitHumanRobotsReduce)

int main(int argc, const char** argv) {
    NYT::Initialize(argc, argv);

    auto client = CreateClient("freud");

    const TString sortedUserTable = "//tmp/" + GetUsername() + "-tutorial-user-sorted-ydl";
    const TString sortedIsRobotTable = "//tmp/" + GetUsername() + "-tutorial-is_robot-sorted-ydl";
    const TString humanTable = "//tmp/" + GetUsername() + "-tutorial-humans-ydl";
    const TString robotTable = "//tmp/" + GetUsername() + "-tutorial-robots-ydl";

    client->Sort(
        TSortOperationSpec()
            .AddInput("//home/ermolovd/yt-tutorial/staff_unsorted-ydl")
            .Output(sortedUserTable)
            .SortBy({"Uid"}));

    client->Sort(
        TSortOperationSpec()
            .AddInput("//home/ermolovd/yt-tutorial/is_robot_unsorted-ydl")
            .Output(sortedIsRobotTable)
            .SortBy({"Uid"}));

    client->Reduce(
        TReduceOperationSpec()
            .ReduceBy({"Uid"})
            .AddInput<NData::TUserRecord>(sortedUserTable)
            .AddInput<NData::TIsRobotRecord>(sortedIsRobotTable)
            .AddOutput<NData::TRobotRecord>(robotTable)
            .AddOutput<NData::THumanRecord>(humanTable),
        new TSplitHumanRobotsReduce);

    Cout << "Robot table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << robotTable << Endl;
    Cout << "Human table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path=" << humanTable << Endl;

    return 0;
}
