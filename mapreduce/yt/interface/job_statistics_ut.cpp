#include <mapreduce/yt/interface/job_statistics.h>
#include <mapreduce/yt/interface/operation.h>

#include <mapreduce/yt/node/node_io.h>

#include <library/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(JobStatistics)
{
    Y_UNIT_TEST(Simple)
    {
        const TString input = R"""(
            {
               "data" = {
                  "output" = {
                      "0" = {
                          "uncompressed_data_size" = {
                              "$" = {
                                  "completed" = {
                                      "simple_sort" = {
                                          "max" = 130;
                                          "count" = 1;
                                          "min" = 130;
                                          "sum" = 130;
                                      };
                                      "map" = {
                                          "max" = 42;
                                          "count" = 1;
                                          "min" = 42;
                                          "sum" = 42;
                                      };
                                  };
                                  "aborted" = {
                                      "simple_sort" = {
                                          "max" = 24;
                                          "count" = 1;
                                          "min" = 24;
                                          "sum" = 24;
                                      };
                                  };
                              };
                          };
                      };
                  };
              };
            })""";

        TJobStatistics stat(NodeFromYsonString(input));

        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Max(), 130);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Count(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Min(), 42);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Sum(), 172);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatistics("data/output/0/uncompressed_data_size").Avg(), 172 / 2);

        UNIT_ASSERT_VALUES_EQUAL(stat.JobState({EJobState::Aborted}).GetStatistics("data/output/0/uncompressed_data_size").Sum(), 24);
        UNIT_ASSERT_VALUES_EQUAL(stat.JobType({EJobType::Map}).JobState({EJobState::Aborted}).GetStatistics("data/output/0/uncompressed_data_size").Sum(), TMaybe<i64>());
    }

    Y_UNIT_TEST(TestOtherTypes)
    {
        const TString input = R"""(
        {
           "time" = {
               "exec" = {
                   "$" = {
                       "completed" = {
                           "map" = {
                               "max" = 2482468;
                               "count" = 38;
                               "min" = 578976;
                               "sum" = 47987270;
                           };
                       };
                   };
               };
           };
        })""";

        TJobStatistics stat(NodeFromYsonString(input));

        UNIT_ASSERT_VALUES_EQUAL(stat.GetStatisticsAs<TDuration>("time/exec").Max(), TDuration::MilliSeconds(2482468));
    }
}
