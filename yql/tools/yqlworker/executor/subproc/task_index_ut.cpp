#include "task_index.h"

#include <library/cpp/testing/unittest/registar.h>


using namespace NYql;
using namespace NProto;
using namespace NSubprocessTaskExecutor;

Y_UNIT_TEST_SUITE(TTaskIndexTest) {
    class TDummyTaskWatcher : public ITaskWatcher {
    public:
        void OnStart() override {}

        void OnFinish(NProto::ETaskStatus status) override {
            Y_UNUSED(status);
        }

        void OnResultChange(
            TTaskResultChanges::TMask changesMask,
            TTaskResultPtr changes,
            int workerPid,
            NBus::TOnMessageContext*) override
        {
            Y_UNUSED(changesMask);
            Y_UNUSED(changes);
            Y_UNUSED(workerPid);
        }

        bool WillWatch(ui32 changesMask) const override {
            Y_UNUSED(changesMask);
            return false;
        }
    };

    static TTaskDataPtr CreateTaskData(const TString& taskName) {
        TTaskDataPtr result = std::make_shared<NProto::TTaskData>();
        result->SetId(taskName);
        result->SetUsername("john");
        return result;
    }

    Y_UNIT_TEST(Empty) {
        TTaskIndex index({});

        TInstant now = TInstant::Now();
        NProto::ETaskAction action = NProto::ETaskAction::UNKNOWN_ACTION;
        TString taskName = "ABC";
        TTaskDataPtr data = CreateTaskData(taskName);
        ITaskWatcherPtr watcher;
        pid_t pid = 999;
        bool isFinished = false;
        TString username;

        UNIT_ASSERT(!index.MarkNextPendingTaskAsRunning(pid, now, action, data));
        UNIT_ASSERT(!index.MarkRunningTaskAsFinished(now, pid, taskName));
        UNIT_ASSERT(!index.FindRunningTaskWatcher(taskName, watcher, pid, isFinished));
        UNIT_ASSERT(!index.HasRunningTask(taskName, isFinished));
        UNIT_ASSERT(!index.RemoveRunningTask(pid, username, watcher, isFinished));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser(username), 0);
    }

    Y_UNIT_TEST(SinglePendingTask) {
        TTaskIndex index({});

        TInstant now = TInstant::Now();
        NProto::ETaskAction action = NProto::ETaskAction::RUN;
        TString taskName = "ABC";
        TTaskDataPtr data = CreateTaskData(taskName);
        ITaskWatcherPtr watcher = new TDummyTaskWatcher();
        pid_t pid = 999;
        bool isFinished = false;
        TString username;

        UNIT_ASSERT(index.PushPendingTask(now, action, data, watcher));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("smith"), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 1);
        // duplicate by task name
        UNIT_ASSERT(!index.PushPendingTask(now, action, data, watcher));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 1);

        // new name
        TTaskDataPtr data2 = CreateTaskData("XYZ");
        UNIT_ASSERT(index.PushPendingTask(now, action, data2, watcher));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 2);

        UNIT_ASSERT(!index.MarkRunningTaskAsFinished(now, pid, taskName));
        UNIT_ASSERT(!index.FindRunningTaskWatcher(taskName, watcher, pid, isFinished));
        UNIT_ASSERT(!index.HasRunningTask(taskName, isFinished));
        UNIT_ASSERT(!index.RemoveRunningTask(pid, username, watcher, isFinished));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 2);
    }

    Y_UNIT_TEST(SingleRunningTask) {
        TTaskIndex index({});

        const TInstant now = TInstant::Now();
        const NProto::ETaskAction action = NProto::ETaskAction::RUN;
        const TString taskName = "ABC";
        const TTaskDataPtr data = CreateTaskData(taskName);
        const ITaskWatcherPtr watcher = new TDummyTaskWatcher();
        const pid_t pid = 999;
        TString username;

        UNIT_ASSERT(index.PushPendingTask(now, action, data, watcher));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 1);

        TInstant pushTime;
        NProto::ETaskAction foundAction = NProto::ETaskAction::UNKNOWN_ACTION;
        TTaskDataPtr foundData = nullptr;

        // special invalid pids: -1, 0
        UNIT_ASSERT(!index.MarkNextPendingTaskAsRunning(-1, pushTime, foundAction, foundData));
        UNIT_ASSERT(!index.MarkNextPendingTaskAsRunning(0, pushTime, foundAction, foundData));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 1);

        UNIT_ASSERT(index.MarkNextPendingTaskAsRunning(pid, pushTime, foundAction, foundData));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(now, pushTime);
        UNIT_ASSERT_EQUAL(action, foundAction);
        UNIT_ASSERT_EQUAL(data, foundData);

        // second attempt with the same pid
        UNIT_ASSERT(!index.MarkNextPendingTaskAsRunning(pid, pushTime, foundAction, foundData));

        // second attempt with new pid
        UNIT_ASSERT(!index.MarkNextPendingTaskAsRunning(pid + 1, pushTime, foundAction, foundData));

        // duplicate by task name even if task is not pending anymore
        UNIT_ASSERT(!index.PushPendingTask(now, action, data, watcher));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 1);

        bool isFinished = false;
        UNIT_ASSERT(!index.HasRunningTask("XYZ", isFinished));
        UNIT_ASSERT(!isFinished);

        UNIT_ASSERT(index.HasRunningTask(taskName, isFinished));
        UNIT_ASSERT(!isFinished);

        pid_t foundPid = 0;
        ITaskWatcherPtr foundWatcher = nullptr;
        UNIT_ASSERT(!index.FindRunningTaskWatcher("XYZ", foundWatcher, foundPid, isFinished));

        UNIT_ASSERT(index.FindRunningTaskWatcher(taskName, foundWatcher, foundPid, isFinished));
        UNIT_ASSERT(!isFinished);
        UNIT_ASSERT_VALUES_EQUAL(foundPid, pid);
        UNIT_ASSERT(watcher == foundWatcher);

        // unknown pid
        UNIT_ASSERT(!index.RemoveRunningTask(pid + 1, username, foundWatcher, isFinished));
        UNIT_ASSERT_VALUES_EQUAL(username, "");
        foundWatcher = nullptr;
        UNIT_ASSERT(index.RemoveRunningTask(pid, username, foundWatcher, isFinished));
        UNIT_ASSERT(!isFinished);
        UNIT_ASSERT(watcher == foundWatcher);
        UNIT_ASSERT_VALUES_EQUAL(username, "john");

        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 0);
    }

    Y_UNIT_TEST(RemovePendingTask) {
        TTaskIndex index({});

        const TInstant now = TInstant::Now();
        const NProto::ETaskAction action = NProto::ETaskAction::RUN;
        const TTaskDataPtr data1 = CreateTaskData("ABC");
        const TTaskDataPtr data2 = CreateTaskData("XYZ");
        const ITaskWatcherPtr watcher = new TDummyTaskWatcher();

        UNIT_ASSERT(index.PushPendingTask(now, action, data1, watcher));
        UNIT_ASSERT(index.PushPendingTask(now, action, data2, watcher));
        UNIT_ASSERT(!index.RemovePendingTask("unknown"));
        UNIT_ASSERT(index.RemovePendingTask("ABC"));
        UNIT_ASSERT(!index.RemovePendingTask("ABC"));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 1);

        TInstant pushTime;
        NProto::ETaskAction foundAction = NProto::ETaskAction::UNKNOWN_ACTION;
        TTaskDataPtr foundData;
        UNIT_ASSERT(index.MarkNextPendingTaskAsRunning(999, pushTime, foundAction, foundData));
        UNIT_ASSERT_EQUAL(foundData, data2);
    }

    Y_UNIT_TEST(AdoptOrphanTask) {
        TTaskIndex index({});

        const TString taskName = "ABC";
        const ITaskWatcherPtr watcher = new TDummyTaskWatcher();
        const pid_t pid = 999;
        TString username = "john";


        UNIT_ASSERT(index.AdoptOrphanTask(999, taskName, username, watcher));

        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 0);

        bool isFinished = false;
        UNIT_ASSERT(index.HasRunningTask("ABC", isFinished));
        UNIT_ASSERT(!isFinished);

        pid_t foundPid = 0;
        ITaskWatcherPtr foundWatcher = nullptr;
        UNIT_ASSERT(index.FindRunningTaskWatcher("ABC", foundWatcher, foundPid, isFinished));
        UNIT_ASSERT(!isFinished);
        UNIT_ASSERT_VALUES_EQUAL(foundPid, pid);
        UNIT_ASSERT(watcher == foundWatcher);

        foundWatcher = nullptr;
        UNIT_ASSERT(index.RemoveRunningTask(pid, username, foundWatcher, isFinished));
        UNIT_ASSERT(!isFinished);
        UNIT_ASSERT(watcher == foundWatcher);
        UNIT_ASSERT_VALUES_EQUAL(username, "john");

        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 0);
    }


    Y_UNIT_TEST(SingleFinishedRunningTask) {
        TTaskIndex index({});

        const TInstant now = TInstant::Now();
        const TInstant finishTime = now + TDuration::Seconds(10);
        const NProto::ETaskAction action = NProto::ETaskAction::RUN;
        const TString taskName = "ABC";
        const TTaskDataPtr data = CreateTaskData(taskName);
        const ITaskWatcherPtr watcher = new TDummyTaskWatcher();
        const pid_t pid = 999;
        TString username;

        UNIT_ASSERT(index.PushPendingTask(now, action, data, watcher));

        TInstant pushTime;
        NProto::ETaskAction foundAction = NProto::ETaskAction::UNKNOWN_ACTION;
        TTaskDataPtr foundData = nullptr;

        UNIT_ASSERT(index.MarkNextPendingTaskAsRunning(pid, pushTime, foundAction, foundData));
        // unknown pid
        UNIT_ASSERT(!index.MarkRunningTaskAsFinished(finishTime, pid + 1, taskName));
        // task does not correspond to pid
        UNIT_ASSERT(!index.MarkRunningTaskAsFinished(finishTime, pid, "XYZ"));

        UNIT_ASSERT(index.MarkRunningTaskAsFinished(finishTime, pid, taskName));

        // duplicate by task name even if task is not pending and finished
        UNIT_ASSERT(!index.PushPendingTask(now, action, data, watcher));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 1);

        bool isFinished = false;
        UNIT_ASSERT(!index.HasRunningTask("XYZ", isFinished));
        UNIT_ASSERT(!isFinished);

        UNIT_ASSERT(index.HasRunningTask(taskName, isFinished));
        UNIT_ASSERT(isFinished);

        isFinished = false;
        pid_t foundPid = 0;
        ITaskWatcherPtr foundWatcher = nullptr;
        UNIT_ASSERT(!index.FindRunningTaskWatcher("XYZ", foundWatcher, foundPid, isFinished));

        UNIT_ASSERT(index.FindRunningTaskWatcher(taskName, foundWatcher, foundPid, isFinished));
        UNIT_ASSERT(isFinished);
        UNIT_ASSERT_VALUES_EQUAL(foundPid, pid);
        UNIT_ASSERT(watcher == foundWatcher);

        isFinished = false;
        // unknown pid
        UNIT_ASSERT(!index.RemoveRunningTask(pid + 1, username, foundWatcher, isFinished));
        foundWatcher = nullptr;
        UNIT_ASSERT(index.RemoveRunningTask(pid, username, foundWatcher, isFinished));
        UNIT_ASSERT(isFinished);
        UNIT_ASSERT(watcher == foundWatcher);
        UNIT_ASSERT_VALUES_EQUAL(username, "john");

        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 0);
    }

    Y_UNIT_TEST(PendingTaskMaxAge) {
        TTaskIndex index({});

        const TInstant t1 = TInstant::Now();
        const TInstant t2 = t1 + TDuration::Seconds(10);
        const TInstant t3 = t1 + TDuration::Seconds(37);

        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingTaskMaxAgeMs(t1), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingTaskMaxAgeMs(t2), 0);

        const NProto::ETaskAction action = NProto::ETaskAction::RUN;
        const TTaskDataPtr data1 = CreateTaskData("ABC");
        const ITaskWatcherPtr watcher = new TDummyTaskWatcher();

        UNIT_ASSERT(index.PushPendingTask(t1, action, data1, watcher));
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingTaskMaxAgeMs(t1), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingTaskMaxAgeMs(t2), 10000);

        const TTaskDataPtr data2 = CreateTaskData("CBZ");
        UNIT_ASSERT(index.PushPendingTask(t2, action, data2, watcher));

        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingTaskMaxAgeMs(t3), 37000);

        const pid_t pid1 = 888;
        NProto::ETaskAction foundAction = NProto::ETaskAction::UNKNOWN_ACTION;
        TTaskDataPtr foundData = nullptr;
        TInstant pushTime;
        UNIT_ASSERT(index.MarkNextPendingTaskAsRunning(pid1, pushTime, foundAction, foundData));

        UNIT_ASSERT_VALUES_EQUAL(t1, pushTime);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingTaskMaxAgeMs(t3), 27000);

        const pid_t pid2 = 999;
        UNIT_ASSERT(index.MarkNextPendingTaskAsRunning(pid2, pushTime, foundAction, foundData));

        UNIT_ASSERT_VALUES_EQUAL(t2, pushTime);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingTaskMaxAgeMs(t3), 0);
    }

    Y_UNIT_TEST(ExtractAllUnfinishedTaskWatchers) {
        TTaskIndex index({});

        const TInstant now = TInstant::Now();
        const auto action = NProto::ETaskAction::RUN;
        const ITaskWatcherPtr pendingWatcher = new TDummyTaskWatcher();
        const ITaskWatcherPtr runningWatcher = new TDummyTaskWatcher();
        const ITaskWatcherPtr finishedWatcher = new TDummyTaskWatcher();

        TInstant pushTime;
        NProto::ETaskAction foundAction = NProto::ETaskAction::UNKNOWN_ACTION;
        TTaskDataPtr foundData;

        UNIT_ASSERT(index.PushPendingTask(now, action, CreateTaskData("running"), runningWatcher));
        UNIT_ASSERT(index.MarkNextPendingTaskAsRunning(101, pushTime, foundAction, foundData));

        UNIT_ASSERT(index.PushPendingTask(now, action, CreateTaskData("finished"), finishedWatcher));
        UNIT_ASSERT(index.MarkNextPendingTaskAsRunning(102, pushTime, foundAction, foundData));
        UNIT_ASSERT(index.MarkRunningTaskAsFinished(now, 102, "finished"));

        UNIT_ASSERT(index.PushPendingTask(now, action, CreateTaskData("pending"), pendingWatcher));
        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(index.GetRunningSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 3);

        const auto watchers = index.ExtractAllUnfinishedTaskWatchers();
        UNIT_ASSERT_VALUES_EQUAL(watchers.size(), 2);
        UNIT_ASSERT(watchers[0] == pendingWatcher);
        UNIT_ASSERT(watchers[1] == runningWatcher);

        UNIT_ASSERT_VALUES_EQUAL(index.GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetPendingSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetRunningSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(index.GetSizeByUser("john"), 0);

        TString username;
        ITaskWatcherPtr watcher;
        bool isFinished = false;
        UNIT_ASSERT(!index.RemoveRunningTask(101, username, watcher, isFinished));
        UNIT_ASSERT(!index.RemoveRunningTask(102, username, watcher, isFinished));
    }
}
