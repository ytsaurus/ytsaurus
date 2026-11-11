#include "task_progress_merger.h"

#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/utils/signals/utils.h>
#ifdef STATUS_PENDING
#undef STATUS_PENDING
#endif
#include <yql/tools/yqlworker/interface/proto/task.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>


using namespace NYql;
const TString Category = "test_category";

Y_UNIT_TEST_SUITE(TTaskProgressCollectorTest) {
    Y_UNIT_TEST(MergeOperationProgress) {
        using EState = TOperationProgress::EState;

        TTaskProgressMerger merger;
        NProto::TTaskProgress taskProgress;

        // first merge/flush

        merger.MergeWith(TOperationProgress(Category, 1, EState::Started, "Starting"));
        merger.MergeWith(TOperationProgress(Category, 2, EState::InProgress, "Running"));
        merger.MergeWith(TOperationProgress(Category, 3, EState::Finished, "Finishing"));
        UNIT_ASSERT(merger.HasChangesSinceLastFlush());

        merger.FlushTo(&taskProgress);
        UNIT_ASSERT(!merger.HasChangesSinceLastFlush());

        UNIT_ASSERT_EQUAL(taskProgress.NodesSize(), 3);
        ui64 firstNodeStartedAt, secondNodeStartedAt;
        {
            auto& nodeProgress = taskProgress.GetNodes(0);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::STARTED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 0);
            UNIT_ASSERT(TInstant::Now().MilliSeconds() - nodeProgress.GetStartedAt() < 100);
            UNIT_ASSERT(!nodeProgress.HasFinishedAt());
            firstNodeStartedAt = nodeProgress.GetStartedAt();

            UNIT_ASSERT_EQUAL(nodeProgress.StagesSize(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(0).GetName(), "Starting");
        }
        {
            auto& nodeProgress = taskProgress.GetNodes(1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::IN_PROGRESS);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 0);
            UNIT_ASSERT(TInstant::Now().MilliSeconds() - nodeProgress.GetStartedAt() < 100);
            UNIT_ASSERT(!nodeProgress.HasFinishedAt());
            secondNodeStartedAt = nodeProgress.GetStartedAt();
            UNIT_ASSERT_EQUAL(nodeProgress.StagesSize(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(0).GetName(), "Running");
        }
        {
            auto& nodeProgress = taskProgress.GetNodes(2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 3);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::FINISHED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 100);
            UNIT_ASSERT(TInstant::Now().MilliSeconds() - nodeProgress.GetStartedAt() < 100);
            UNIT_ASSERT(TInstant::Now().MilliSeconds() - nodeProgress.GetFinishedAt() < 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), nodeProgress.GetFinishedAt());
            UNIT_ASSERT_EQUAL(nodeProgress.StagesSize(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(0).GetName(), "Finishing");
        }

        // second merge/flush

        merger.MergeWith(TOperationProgress(Category, 1, EState::InProgress, "Running"));
        merger.MergeWith(TOperationProgress(Category, 2, EState::Finished, "Finishing"));
        UNIT_ASSERT(merger.HasChangesSinceLastFlush());

        taskProgress.Clear();
        merger.FlushTo(&taskProgress);
        UNIT_ASSERT(!merger.HasChangesSinceLastFlush());

        UNIT_ASSERT_EQUAL(taskProgress.NodesSize(), 2);
        {
            auto& nodeProgress = taskProgress.GetNodes(0);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::IN_PROGRESS);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 0);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), firstNodeStartedAt);
            UNIT_ASSERT(!nodeProgress.HasFinishedAt());
            UNIT_ASSERT_EQUAL(nodeProgress.StagesSize(), 2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(0).GetName(), "Starting");
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(1).GetName(), "Running");
        }
        {
            auto& nodeProgress = taskProgress.GetNodes(1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::FINISHED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), secondNodeStartedAt);
            UNIT_ASSERT(TInstant::Now().MilliSeconds() - nodeProgress.GetFinishedAt() < 100);
            UNIT_ASSERT_EQUAL(nodeProgress.StagesSize(), 2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(0).GetName(), "Running");
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(1).GetName(), "Finishing");
        }

        // third merge/flush

        merger.MergeWith(TOperationProgress(Category, 1, EState::Finished, "Finishing"));
        UNIT_ASSERT(merger.HasChangesSinceLastFlush());

        taskProgress.Clear();
        merger.FlushTo(&taskProgress);
        UNIT_ASSERT(!merger.HasChangesSinceLastFlush());

        UNIT_ASSERT_EQUAL(taskProgress.NodesSize(), 1);
        {
            auto& nodeProgress = taskProgress.GetNodes(0);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::FINISHED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), firstNodeStartedAt);
            UNIT_ASSERT(TInstant::Now().MilliSeconds() - nodeProgress.GetFinishedAt() < 100);
            UNIT_ASSERT_EQUAL(nodeProgress.StagesSize(), 3);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(0).GetName(), "Starting");
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(1).GetName(), "Running");
            UNIT_ASSERT_EQUAL(nodeProgress.GetStages(2).GetName(), "Finishing");
        }
    }

    void AddNode(NProto::TTaskProgress* taskProgress,
                 ui32 id, NProto::TTaskProgress::ENodeState state,
                 ui32 current, ui32 total,
                 ui64 startedAt, TMaybe<ui64> finishedAt = Nothing())
    {
        auto* node = taskProgress->AddNodes();
        node->SetId(id);
        node->SetState(state);
        node->SetCurrent(current);
        node->SetTotal(total);
        node->SetStartedAt(startedAt);
        if (finishedAt) {
            node->SetFinishedAt(*finishedAt);
        }
    }

    Y_UNIT_TEST(MergeTaskProgres) {
        TTaskProgressMerger merger;
        NProto::TTaskProgress taskProgress;

        // first merge/flush

        {
            NProto::TTaskProgress update;
            AddNode(&update, 1, NProto::TTaskProgress::STARTED,       0, 100, 0);
            AddNode(&update, 2, NProto::TTaskProgress::IN_PROGRESS,  30, 100, 1);
            AddNode(&update, 3, NProto::TTaskProgress::FINISHED,    100, 100, 2, 3);
            merger.MergeWith(update);
        }
        UNIT_ASSERT(merger.HasChangesSinceLastFlush());

        merger.FlushTo(&taskProgress);
        UNIT_ASSERT(!merger.HasChangesSinceLastFlush());

        UNIT_ASSERT_EQUAL(taskProgress.NodesSize(), 3);
        ui64 firstNodeStartedAt, secondNodeStartedAt;
        {
            auto& nodeProgress = taskProgress.GetNodes(0);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::STARTED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 0);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetTotal(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), 0);
            UNIT_ASSERT(!nodeProgress.HasFinishedAt());
            firstNodeStartedAt = nodeProgress.GetStartedAt();
        }
        {
            auto& nodeProgress = taskProgress.GetNodes(1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::IN_PROGRESS);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 30);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetTotal(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), 1);
            UNIT_ASSERT(!nodeProgress.HasFinishedAt());
            secondNodeStartedAt = nodeProgress.GetStartedAt();
        }
        {
            auto& nodeProgress = taskProgress.GetNodes(2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 3);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::FINISHED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 100);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetTotal(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), 2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetFinishedAt(), 3);
        }

        // second merge/flush

        {
            NProto::TTaskProgress update;
            AddNode(&update, 1, NProto::TTaskProgress::IN_PROGRESS,  30, 100, 10);
            AddNode(&update, 2, NProto::TTaskProgress::FINISHED,    100, 100, 11, 12);
            merger.MergeWith(update);
        }
        UNIT_ASSERT(merger.HasChangesSinceLastFlush());

        taskProgress.Clear();
        merger.FlushTo(&taskProgress);
        UNIT_ASSERT(!merger.HasChangesSinceLastFlush());

        UNIT_ASSERT_EQUAL(taskProgress.NodesSize(), 2);
        {
            auto& nodeProgress = taskProgress.GetNodes(0);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::IN_PROGRESS);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 30);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetTotal(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), firstNodeStartedAt);
            UNIT_ASSERT(!nodeProgress.HasFinishedAt());
        }
        {
            auto& nodeProgress = taskProgress.GetNodes(1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 2);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::FINISHED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 100);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetTotal(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), secondNodeStartedAt);
            UNIT_ASSERT_EQUAL(nodeProgress.GetFinishedAt(), 12);
        }

        // third merge/flush

        {
            NProto::TTaskProgress update;
            AddNode(&update, 1, NProto::TTaskProgress::FINISHED, 100, 100, 20, 21);
            AddNode(&update, 4, NProto::TTaskProgress::STARTED,    0, 100, 22);
            merger.MergeWith(update);
        }
        UNIT_ASSERT(merger.HasChangesSinceLastFlush());

        taskProgress.Clear();
        merger.FlushTo(&taskProgress);
        UNIT_ASSERT(!merger.HasChangesSinceLastFlush());

        UNIT_ASSERT_EQUAL(taskProgress.NodesSize(), 2);
        {
            auto& nodeProgress = taskProgress.GetNodes(0);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::FINISHED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 100);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetTotal(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), firstNodeStartedAt);
            UNIT_ASSERT_EQUAL(nodeProgress.GetFinishedAt(), 21);
        }
        {
            auto& nodeProgress = taskProgress.GetNodes(1);
            UNIT_ASSERT_EQUAL(nodeProgress.GetId(), 4);
            UNIT_ASSERT_EQUAL(nodeProgress.GetState(), NProto::TTaskProgress::STARTED);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetCurrent(), 0);
            //UNIT_ASSERT_EQUAL(nodeProgress.GetTotal(), 100);
            UNIT_ASSERT_EQUAL(nodeProgress.GetStartedAt(), 22);
            UNIT_ASSERT(!nodeProgress.HasFinishedAt());
        }
    }
}
