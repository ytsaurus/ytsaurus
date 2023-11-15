#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/library/operation_tracker/operation_tracker.h> // OperationTracker живёт в этой библиотеке

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

int main() {
    NYT::Initialize();

    auto client = CreateClient("freud");

    const TString sortedLoginTable = "//tmp/" + GetUsername() + "-tutorial-login-sorted";
    const TString sortedIsRobotTable = "//tmp/" + GetUsername() + "-tutorial-is_robot-sorted";
    const TString humanTable = "//tmp/" + GetUsername() + "-tutorial-humans";
    const TString robotTable = "//tmp/" + GetUsername() + "-tutorial-robots";

    TOperationTracker tracker;
    tracker.AddOperation(
        client->Sort(
            TSortOperationSpec()
                .AddInput("//home/tutorial/staff_unsorted")
                .Output(sortedLoginTable)
                .SortBy({"uid"}),
            TOperationOptions().Wait(false))); // Wait(false) говорит клиенту не ждать завершения операции

    tracker.AddOperation(
        client->Sort(
            TSortOperationSpec()
                .AddInput("//home/tutorial/is_robot_unsorted")
                .Output(sortedIsRobotTable)
                .SortBy({"uid"}),
            TOperationOptions().Wait(false)));

    tracker.WaitAllCompleted(); // Ждёт когда обе операции завершатся, бросит исключение в случае ошибок.

    return 0;
}
