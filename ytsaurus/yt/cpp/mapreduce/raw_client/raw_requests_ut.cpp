#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NYT::NDetail::NRawClient;

Y_UNIT_TEST_SUITE(OperationsApiParsing)
{
    Y_UNIT_TEST(ParseOperationAttributes)
    {
        auto response = TStringBuf(R"""({
            "id" = "1-2-3-4";
            "authenticated_user" = "some-user";
            "start_time" = "2018-01-01T00:00:00.0Z";
            "weight" = 1.;
            "state" = "completed";
            "suspended" = %false;
            "finish_time" = "2018-01-02T00:00:00.0Z";
            "brief_progress" = {
                "jobs" = {
                    "lost" = 0;
                    "pending" = 0;
                    "failed" = 1;
                    "aborted" = 0;
                    "total" = 84;
                    "running" = 0;
                    "completed" = 84;
                };
            };
            "result" = {
                "error" = {
                    "attributes" = {};
                    "code" = 0;
                    "message" = "";
                };
            };
            "brief_spec" = {
                "input_table_paths" = <
                    "count" = 1;
                > [
                    "//some-input";
                ];
                "pool" = "some-pool";
                "scheduling_info_per_pool_tree" = {
                    "physical" = {
                        "pool" = "some-pool";
                    };
                };
                "title" = "some-title";
                "output_table_paths" = <
                    "count" = 1;
                > [
                    "//some-output";
                ];
                "mapper" = {
                    "command" = "some-command";
                };
            };
            "type" = "map";
            "pool" = "some-pool";
            "progress" = {
                "build_time" = "2018-01-01T00:00:00.000000Z";
                "job_statistics" = {
                    "data" = {
                        "input" = {
                            "row_count" = {
                                "$" = {
                                    "failed" = {
                                        "map" = {
                                            "max" = 1;
                                            "count" = 1;
                                            "sum" = 1;
                                            "min" = 1;
                                        };
                                    };
                                    "completed" = {
                                        "map" = {
                                            "max" = 1;
                                            "count" = 84;
                                            "sum" = 84;
                                            "min" = 1;
                                        };
                                    };
                                };
                            };
                        };
                    };
                };
                "total_job_counter" = {
                    "completed" = {
                        "total" = 3;
                        "non-interrupted" = 1;
                        "interrupted" = {
                            "whatever_interrupted" = 2;
                        };
                    };
                    "aborted" = {
                        "non_scheduled" = {
                            "whatever_non_scheduled" = 3;
                        };
                        "scheduled" = {
                            "whatever_scheduled" = 4;
                        };
                        "total" = 7;
                    };
                    "lost" = 5;
                    "invalidated" = 6;
                    "failed" = 7;
                    "running" = 8;
                    "suspended" = 9;
                    "pending" = 10;
                    "blocked" = 11;
                    "total" = 66;
                };
            };
            "events" = [
                {"state" = "pending"; "time" = "2018-01-01T00:00:00.000000Z";};
                {"state" = "materializing"; "time" = "2018-01-02T00:00:00.000000Z";};
                {"state" = "running"; "time" = "2018-01-03T00:00:00.000000Z";};
            ];
        })""");
        auto attrs = ParseOperationAttributes(NodeFromYsonString(response));

        UNIT_ASSERT(attrs.Id);
        UNIT_ASSERT_VALUES_EQUAL(GetGuidAsString(*attrs.Id), "1-2-3-4");

        UNIT_ASSERT(attrs.Type);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.Type, EOperationType::Map);

        UNIT_ASSERT(attrs.State);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.State, "completed");

        UNIT_ASSERT(attrs.BriefState);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.BriefState, EOperationBriefState::Completed);

        UNIT_ASSERT(attrs.AuthenticatedUser);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.AuthenticatedUser, "some-user");

        UNIT_ASSERT(attrs.StartTime);
        UNIT_ASSERT(attrs.FinishTime);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.FinishTime - *attrs.StartTime, TDuration::Days(1));

        UNIT_ASSERT(attrs.BriefProgress);
        UNIT_ASSERT_VALUES_EQUAL(attrs.BriefProgress->Completed, 84);
        UNIT_ASSERT_VALUES_EQUAL(attrs.BriefProgress->Failed, 1);

        UNIT_ASSERT(attrs.BriefSpec);
        UNIT_ASSERT_VALUES_EQUAL((*attrs.BriefSpec)["title"].AsString(), "some-title");

        UNIT_ASSERT(attrs.Suspended);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.Suspended, false);

        UNIT_ASSERT(attrs.Result);
        UNIT_ASSERT(!attrs.Result->Error);

        UNIT_ASSERT(attrs.Progress);
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobStatistics.JobState({}).GetStatistics("data/input/row_count").Sum(), 85);
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobCounters.GetCompletedInterrupted().GetTotal(), 2);
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobCounters.GetAbortedNonScheduled().GetTotal(), 3);
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobCounters.GetAbortedScheduled().GetTotal(), 4);
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobCounters.GetAborted().GetTotal(), 7);
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobCounters.GetFailed().GetTotal(), 7);
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobCounters.GetTotal(), 66);
        UNIT_ASSERT_VALUES_EQUAL(*attrs.Progress->BuildTime, TInstant::ParseIso8601("2018-01-01T00:00:00.000000Z"));

        UNIT_ASSERT(attrs.Events);
        UNIT_ASSERT_VALUES_EQUAL((*attrs.Events)[1].State, "materializing");
        UNIT_ASSERT_VALUES_EQUAL((*attrs.Events)[1].Time, TInstant::ParseIso8601("2018-01-02T00:00:00.000000Z"));
    }

    Y_UNIT_TEST(EmptyProgress)
    {
        auto response = TStringBuf(R"""({
            "id" = "1-2-3-4";
            "brief_progress" = {};
            "progress" = {};
        })""");
        auto attrs = ParseOperationAttributes(NodeFromYsonString(response));

        UNIT_ASSERT(attrs.Id);
        UNIT_ASSERT_VALUES_EQUAL(GetGuidAsString(*attrs.Id), "1-2-3-4");

        UNIT_ASSERT(!attrs.BriefProgress);

        UNIT_ASSERT(attrs.Progress);
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobStatistics.JobState({}).GetStatisticsNames(), TVector<TString>{});
        UNIT_ASSERT_VALUES_EQUAL(attrs.Progress->JobCounters.GetTotal(), 0);
        UNIT_ASSERT(!attrs.Progress->BuildTime);
    }
}
