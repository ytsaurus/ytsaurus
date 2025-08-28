#include <yt/yt/server/lib/chunk_pools/job_size_tracker.h>

#include <yt/yt/server/lib/chunk_pools/unittests/chunk_pools_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChunkPools {
namespace {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TJobSizeTrackerTest
    : public TChunkPoolTestBase
{
protected:
    TLogger Logger = ChunkPoolLogger();

    static TResourceVector CreateVector(
        i64 dataSliceCount = 0,
        i64 dataWeight = 0,
        i64 primaryDataWeight = 0)
    {
        TResourceVector vector;
        vector.Values[EResourceKind::DataSliceCount] = dataSliceCount;
        vector.Values[EResourceKind::DataWeight] = dataWeight;
        vector.Values[EResourceKind::PrimaryDataWeight] = primaryDataWeight;
        return vector;
    }

    static TResourceVector CreateDataWeightVector(
        i64 dataWeight = 0,
        i64 primaryDataWeight = 0)
    {
        TResourceVector vector;
        vector.Values[EResourceKind::DataWeight] = dataWeight;
        vector.Values[EResourceKind::PrimaryDataWeight] = primaryDataWeight;
        return vector;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobSizeTrackerTest, AccountingBySingleComponent)
{
    // Legend:
    //   DOM        - Dominant resource
    //   RUN        - Cumulative run index (increases when dominant resource changes)
    //   JOB        - Job index (job is created when CheckOverflow returns true)
    //   Extra      - Resource vector of slice being added
    //   Local      - Resource usage for current job
    //   Cumulative - Total resource usage since run start
    //   Limit      - Current limits (HystLocal is 1.5x LocalLimit)
    //
    // Vector format: [DSC][DW]
    // +---+---+---+------------+-------------------------+--------------------------------------+
    // | D | R | J |   Extra    |          Usage          |                Limit                 |
    // | O | U | O +------------+------------+------------+------------+------------+------------+
    // | M | N | B |            |   Local    | Cumulative | HystLocal  | Cumulative |   Local    |
    // +---+---+---+------------+------------+------------+------------+------------+------------+
    // |   |   |   |            | [ 0][  0]  | [ 0][  0]  |            |            |            |
    // |   |   | 1 | [ 1][ 50]  | [ 1][ 50]  | [ 1][ 50]  | [inf][inf] | [inf][100] | [inf][100] |
    // |   |   |   | [ 1][ 50]  | [ 2][100]  | [ 2][100]  |            |            |            |
    // |   |   +---+------------+------------+------------+------------+------------+------------+
    // |   |   |   |            | [ 0][  0]  | [ 2][100]  | [inf][inf] | [inf][200] | [inf][100] |
    // | D |   | 2 | [ 1][150]  | [ 1][150]  | [ 3][250]  |            |            |            |
    // | W | 1 +---+------------+------------+------------+------------+------------+------------+
    // |   |   |   |            | [ 0][  0]  | [ 3][250]  | [inf][inf] | [inf][300] | [inf][100] |
    // |   |   | 3 | [ 1][500]  | [ 1][500]  | [ 4][750]  |            |            |            |
    // |   |   +---+------------+------------+------------+------------+------------+------------+
    // |   |   |   |            | [ 0][  0]  | [ 4][750]  | [inf][inf] | [inf][400] | [inf][100] |
    // |   |   | 4 | [ 1][  1]  | [ 1][  1]  | [ 5][751]  |            |            |            |
    // +---+---+---+------------+------------+------------+------------+------------+------------+

    // When one resource type dominates, jobs are split to converge average weight to data_weight_per_job.
    // This can create inefficiently small jobs (see job 4) that still trigger overflow,
    // potentially wasting cluster resources.
    //
    // Asymmetric overflow behavior: Any amount of dominant resource (even tiny) can cause
    // overflow, but there's no upper bound that always fits. The dominant resource's
    // cumulative limit tracks its usage exactly - faster growth would trigger overflow
    // on another resource.

    auto tracker = CreateJobSizeTracker(
        /*limitVector*/ CreateVector(std::numeric_limits<i64>::max(), 100),
        TJobSizeTrackerOptions(),
        Logger);

    // Job 1: Two slices within limit.

    {
        auto resources = CreateVector(1, 50);
        ASSERT_FALSE(tracker->CheckOverflow(resources));
        tracker->AccountSlice(resources);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
    }

    {
        auto resources = CreateVector(1, 50);
        ASSERT_FALSE(tracker->CheckOverflow(resources));
        tracker->AccountSlice(resources);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
    }

    // Job 2: Exceeds limit, triggers new job.

    {
        auto resources = CreateVector(1, 150);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
    }

    // Job 3: Large slice exceeds limit.

    {
        auto resources = CreateVector(1, 500);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
    }

    // Job 4: Even tiny slice triggers overflow.

    {
        auto resources = CreateVector(1, 1);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
    }

    // Bug? After flush, even empty slice causes overflow.
    ASSERT_TRUE(tracker->CheckOverflow());
}

TEST_F(TJobSizeTrackerTest, AccountingByTwoEqualComponents)
{
    // Vector format: [DW][PDW]
    // +---+---+---+------------+-------------------------+--------------------------------------+
    // | D | R | J |   Extra    |          Usage          |                Limit                 |
    // | O | U | O +------------+------------+------------+------------+------------+------------+
    // | M | N | B |            |   Local    | Cumulative | HystLocal  | Cumulative |   Local    |
    // +---+---+---+------------+------------+------------+------------+------------+------------+
    // |   |   |   |            | [  0][  0] | [  0][  0] |            |            |            |
    // |   |   | 1 | [ 50][ 50] | [ 50][ 50] | [ 50][ 50] | [inf][150] | [100][100] | [100][100] |
    // | D |   |   | [ 50][ 50] | [100][100] | [100][100] |            |            |            |
    // | W | 1 +---+------------+------------+------------+------------+------------+------------+
    // |   |   |   |            | [  0][  0] | [100][100] |            |            |            |
    // |   |   | 2 | [ 20][ 20] | [ 20][ 20] | [120][120] | [inf][150] | [200][200] | [100][100] |
    // +---+---+---+------------+------------+------------+------------+------------+------------+
    // |   |   |   |            | [  0][  0] | [  0][  0] |            |            |            |
    // | P |   | 3 | [150][150] | [150][150] | [150][150] | [150][inf] | [100][100] | [100][100] |
    // | D | 2 +---+------------+------------+------------+------------+------------+------------+
    // | W |   |   |            | [  0][  0] | [150][150] |            |            |            |
    // |   |   | 4 | [  1][  1] | [  1][  1] | [151][151] | [150][inf] | [200][200] | [100][100] |
    // +---+---+---+------------+------------+------------+------------+------------+------------+
    // | D |   |   |            | [  0][  0] | [  0][  0] |            |            |            |
    // | W | 3 | 5 | [500][500] | [500][500] | [500][500] | [inf][150] | [100][100] | [100][100] |
    // +---+---+---+------------+-------------------------+--------------------------------------+

    // When DataWeight and PrimaryDataWeight are equal, new run starts when:
    // (slice + current_job_data_weight) > data_weight_per_job * 1.5
    //
    // This causes frequent dominant resource switches. Example from production:
    // 754 switches across 837 jobs (switching every 1.1 jobs).
    // This behavior may prevent creation of very small jobs.

    auto tracker = CreateJobSizeTracker(
        /*limitVector*/ CreateDataWeightVector(100, 100),
        TJobSizeTrackerOptions(),
        Logger);

    // Job 1: Two equal slices.

    {
        auto resources = CreateDataWeightVector(50, 50);
        ASSERT_FALSE(tracker->CheckOverflow(resources));
        tracker->AccountSlice(resources);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
    }

    {
        auto resources = CreateDataWeightVector(50, 50);
        ASSERT_FALSE(tracker->CheckOverflow(resources));
        tracker->AccountSlice(resources);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
    }

    // Job 2: Small slice triggers new job.

    {
        auto resources = CreateDataWeightVector(20, 20);

        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
    }

    // Job 3: Large slice switches dominant resource.

    {
        auto resources = CreateDataWeightVector(150, 150);

        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
    }

    // Job 4: Tiny slice still triggers overflow.

    {
        auto resources = CreateDataWeightVector(1, 1);

        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
    }

    // Job 5: Another large slice.

    {
        auto resources = CreateDataWeightVector(500, 500);

        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
    }
}

TEST_F(TJobSizeTrackerTest, MultipleResourceCompetition)
{
    // Test behavior when multiple non-dominant resources compete for overflow
    //
    // Vector format: [DSC][DW][PDW]
    // +---+---+---+---------------+-------------------------------+--------------------------------------------------+
    // | D | R | J |     Extra     |            Usage              |                      Limit                       |
    // | O | U | O +---------------+---------------+---------------+-----------------+----------------+---------------+
    // | M | N | B |               |     Local     |  Cumulative   |   HystLocal     |   Cumulative   |     Local     |
    // +---+---+---+---------------+---------------+---------------+-----------------+----------------+---------------+
    // |   |   |   |               | [ 0][ 0][ 0]  | [ 0][  0][ 0] |                 |                |               |
    // |   |   | 1 | [ 5][ 30][20] | [ 5][30][20]  | [ 5][ 30][20] | [15 ][inf][ 75] | [10][100][ 50] | [10][100][50] |
    // | D |   |   | [ 4][ 30][25] | [ 9][60][45]  | [ 9][ 60][45] |                 |                |               |
    // | W | 1 +---+---------------+---------------+---------------+-----------------+----------------+---------------+
    // |   |   |   |               | [ 0][  0][ 0] | [ 9][ 60][45] |                 |                |               |
    // |   |   | 2 | [ 3][140][ 5] | [ 3][140][ 5] | [12][200][50] | [15 ][inf][ 75] | [20][200][100] | [10][100][50] |
    // +---+---+---+---------------+---------------+---------------+-----------------+----------------+---------------+
    // | P |   |   |               |               |               |                 |                |               |
    // | D |   |   |               | [ 0][  0][ 0] | [ 0][  0][ 0] |                 |                |               |
    // | W | 2 | 3 | [ 3][ 20][80] | [ 3][ 20][80] | [ 3][ 20][80] | [15 ][150][inf] | [10][100][ 50] | [10][100][50] |
    // +---+---+---+---------------+---------------+---------------+-----------------+----------------+---------------+
    // | D |   |   |               |               |               |                 |                |               |
    // | S |   |   |               | [ 0][  0][ 0] | [ 0][  0][ 0] |                 |                |               |
    // | C | 3 | 4 | [13][  0][ 0] | [13][  0][ 0] | [13][  0][ 0] | [inf][150][ 75] | [10][100][ 50] | [10][100][50] |
    // +---+---+---+---------------+---------------+---------------+-----------------+----------------+---------------+

    auto tracker = CreateJobSizeTracker(
        /*limitVector*/ CreateVector(10, 100, 50),
        TJobSizeTrackerOptions(),
        Logger);

    // Job 1: Balanced usage within limits.
    {
        auto resources = CreateVector(5, 30, 20);
        ASSERT_FALSE(tracker->CheckOverflow(resources));
        tracker->AccountSlice(resources);

        resources = CreateVector(4, 30, 25);
        ASSERT_FALSE(tracker->CheckOverflow(resources));
        tracker->AccountSlice(resources);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
    }

    // Job 2: DataWeight causes cumulative overflow (stays dominant).
    {
        auto resources = CreateVector(3, 140, 5);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);

        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow(resources).has_value());
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
    }

    // Job 3: CompressedDataSize overflow switches dominant resource.
    {
        auto resources = CreateVector(3, 20, 80);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);

        // This should switch dominant resource to PDW.
        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
    }

    // Job 4: DataSliceCount would reach hysteresis limit.
    {
        auto resources = CreateVector(13, 0, 0);
        auto token = tracker->CheckOverflow(resources);
        // Should overflow because DSC would reach the hysteresis limit (13 + 3 = 16)
        ASSERT_TRUE(token);

        // This should switch dominant resource to DSC.
        tracker->Flush(token);
        ASSERT_FALSE(tracker->CheckOverflow().has_value());
        tracker->AccountSlice(resources);
        ASSERT_TRUE(tracker->CheckOverflow().has_value());
    }
}

TEST_F(TJobSizeTrackerTest, SuggestRowSplitFractionEdgeCases)
{
    // Test SuggestRowSplitFraction with various edge cases
    auto tracker = CreateJobSizeTracker(
        /*limitVector*/ CreateDataWeightVector(100, 100),
        TJobSizeTrackerOptions(),
        Logger);

    // Fill up to near limit
    tracker->AccountSlice(CreateDataWeightVector(90, 90));

    // Test 1: Resource that would overflow both limits.
    {
        double fraction = tracker->SuggestRowSplitFraction(CreateDataWeightVector(100, 100));
        // Limited by cumulative gap: (100-90)/100 = 0.1
        // NOT by hysteresis limit because cumulative limit is stricter.
        ASSERT_NEAR(fraction, 0.1, 0.01);
    }

    // Test 2: One resource would overflow.
    {
        double fraction = tracker->SuggestRowSplitFraction(CreateDataWeightVector(20, 100));
        // Limited by PDW: (100-90)/100 = 0.1
        ASSERT_NEAR(fraction, 0.1, 0.01);
    }

    // Test 3: Zero vector.
    {
        double fraction = tracker->SuggestRowSplitFraction(CreateDataWeightVector(0, 0));
        // Division by zero should be handled.
        ASSERT_EQ(fraction, 1.0);
    }

    // Test 4: After flush, hysteresis limit becomes relevant.
    {
        // Need to trigger overflow first by adding something
        auto token = tracker->CheckOverflow(CreateDataWeightVector(20, 20));
        ASSERT_TRUE(token);
        tracker->Flush(token);

        // Now cumulative limit is 200 for both.
        // Local usage is 0, cumulative usage is 90.
        double fraction = tracker->SuggestRowSplitFraction(CreateDataWeightVector(105, 105));
        // Limited by cumulative gap: (200-90)/105 ~ 1.04, but should be capped at 1.0.
        ASSERT_EQ(fraction, 1.0);
    }

    // Test 5: Fill local partially after flush.
    {
        tracker->AccountSlice(CreateDataWeightVector(50, 50));

        double fraction = tracker->SuggestRowSplitFraction(CreateDataWeightVector(200, 200));
        // Local gap: (150-50)/200 = 0.5
        // Cumulative gap: (200-140)/200 = 0.3
        // Combined uses minimum: 0.3
        ASSERT_NEAR(fraction, 0.3, 0.01);
    }

    // Test 6: Negative gap (overflow already happened).
    {
        tracker->AccountSlice(CreateDataWeightVector(200, 200));

        double fraction = tracker->SuggestRowSplitFraction(CreateDataWeightVector(100, 100));
        // Both local and cumulative are exceeded, should return 0.
        ASSERT_EQ(fraction, 0.0);
    }
}

TEST_F(TJobSizeTrackerTest, LimitProgressionRatio_2_0)
{
    // Test geometric limit progression with ratio 2.0.
    // Vector format: [DW][PDW]
    // +---+---+---+------------+-------------------------+--------------------------------------+
    // | D | R | J |   Extra    |          Usage          |                Limit                 |
    // | O | U | O +------------+------------+------------+------------+------------+------------+
    // | M | N | B |            |   Local    | Cumulative | HystLocal  | Cumulative |   Local    |
    // +---+---+---+------------+------------+------------+------------+------------+------------+
    // |   |   | - | [120][120] | [  0][  0] | [  0][  0] | [inf][150] | [100][100] | [100][100] |
    // | D |   +---+------------+------------+------------+------------+------------+------------+
    // | W |   |   |            | [  0][  0] | [  0][  0] | [inf][300] | [300][300] | [200][200] |
    // |   | 1 | 1 | [150][150] | [150][150] | [150][150] |            |            | x2 applied |
    // +---+---+---+------------+------------+------------+------------+------------+------------+
    // | P |   |   |            |            |            |            |            |            |
    // | D |   |   |            | [  0][  0] | [  0][  0] | [600][inf] | [400][400] | [400][400] |
    // | W | 2 | 2 | [350][350] | [350][350] | [350][350] |            |            | x2 applied |
    // +---+---+---+------------+------------+------------+------------+------------+------------+
    // | D |   |   |            | [  0][  0] | [  0][  0] | [inf][600] | [400][400] | [400][400] |
    // | W | 3 | - | [500][500] | [  0][  0] | [  0][  0] |            |            | no x2      |
    // +---+---+---+------------+------------+------------+------------+------------+------------+

    TJobSizeTrackerOptions options;
    options.LimitProgressionRatio = 2.0;
    options.LimitProgressionLength = 3;
    options.GeometricResources = {EResourceKind::DataWeight, EResourceKind::PrimaryDataWeight};

    auto tracker = CreateJobSizeTracker(
        /*limitVector*/ CreateDataWeightVector(100, 100),
        options,
        Logger);

    // Job 1: Try to add slice that triggers overflow.
    {
        auto resources = CreateDataWeightVector(120, 120);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        // Slice not added since it would overflow.
        tracker->Flush(token);
        // After flush: limits doubled to 200, cumulative increases.
    }

    // Job 2: Add data within new limits.
    {
        auto resources = CreateDataWeightVector(150, 150);
        ASSERT_FALSE(tracker->CheckOverflow(resources));
        tracker->AccountSlice(resources);

        // Try to add more, triggers overflow.
        resources = CreateDataWeightVector(250, 250);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        // After flush: limits doubled to 400, dominant resource switches to PDW.
    }

    // Job 3: Add data within new limits.
    {
        auto resources = CreateDataWeightVector(350, 350);
        ASSERT_FALSE(tracker->CheckOverflow(resources));
        tracker->AccountSlice(resources);

        // Try to add more, triggers overflow
        resources = CreateDataWeightVector(500, 500);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        // Progression limit reached (3/3), no more doubling.
    }

    // Job 4: Verify no further progression
    {
        auto resources = CreateDataWeightVector(500, 500);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);  // Should still overflow at 400 limit.
    }
}

TEST_F(TJobSizeTrackerTest, LimitProgressionWithOffset)
{
    // Test progression offset delays limit increases
    // Vector format: [DW][PDW]
    // +---+------------+--------------------------------------+
    // | R |   Extra    |                Limit                 |
    // | U |------------+------------+------------+------------+
    // | N |            | HystLocal  | Cumulative |   Local    |
    // +---+------------+------------+------------+------------+
    // | 1 | [120][120] | [inf][150] | [100][100] | [100][100] |
    // +---+------------+------------+------------+------------+
    // | 2 | [250][250] | [150][inf] | [200][200] | [100][100] |
    // +---+------------+------------+------------+------------+
    // |   | [250][250] | [inf][300] | [200][200] | [200][200] |
    // | 3 |            |            |            | x2 applied |
    // +---+------------+------------+------------+------------+

    TJobSizeTrackerOptions options;
    options.LimitProgressionRatio = 2.0;
    options.LimitProgressionLength = 2;
    options.LimitProgressionOffset = 2;  // Skip first 2 flushes.
    options.GeometricResources = {EResourceKind::DataWeight, EResourceKind::PrimaryDataWeight};

    auto tracker = CreateJobSizeTracker(
        /*limitVector*/ CreateDataWeightVector(100, 100),
        options,
        Logger);

    // Job 1: First flush - no progression (offset: 1/2).
    {
        auto resources = CreateDataWeightVector(120, 120);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        // Limits stay at 100, cumulative increases to 200.
    }

    // Job 2: Second flush - no progression (offset: 2/2).
    {
        auto resources = CreateDataWeightVector(250, 250);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        // Limits still at 100, dominant switches to PDW.
    }

    // Job 3: Third flush - progression starts.
    {
        auto resources = CreateDataWeightVector(250, 250);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        // Now limits doubled to 200, dominant switches back to DW.
    }

    // Verify progression happened.
    {
        auto resources = CreateDataWeightVector(190, 190);
        ASSERT_FALSE(tracker->CheckOverflow(resources));  // Fits in new limit.
        tracker->AccountSlice(resources);

        // Verify exact limit.
        resources = CreateDataWeightVector(100, 100);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);  // 190 + 100 > 200 cumulative limit.
    }
}

TEST_F(TJobSizeTrackerTest, LimitProgressionOnlyForGeometricResources)
{
    // Test that only specified resources are affected by progression
    // Vector format: [DSC][DW][PDW]
    // +---+---------------+--------------------------------------------------+
    // | R |     Extra     |                      Limit                       |
    // | U +---------------+-----------------+----------------+---------------+
    // | N |               |   HystLocal     |   Cumulative   |     Local     |
    // +---+---------------+-----------------+----------------+---------------+
    // |   | [ 5][120][ 0] | [15][inf][150]  | [10][100][100] | [10][100][100]|
    // | 1 +---------------+-----------------+----------------+---------------+
    // |   |               | [15][inf][300]  | [10][200][200] | [10][200][200]|
    // |   | [16][ 0][ 0]  |                 |                | DSC unchanged |
    // +---+---------------+-----------------+----------------+---------------+

    TJobSizeTrackerOptions options;
    options.LimitProgressionRatio = 2.0;
    options.LimitProgressionLength = 2;
    options.GeometricResources = {EResourceKind::DataWeight, EResourceKind::PrimaryDataWeight};
    // Note: DataSliceCount is NOT in GeometricResources.

    auto tracker = CreateJobSizeTracker(
        /*limitVector*/ CreateVector(10, 100, 100),
        options,
        Logger);

    // Job 1: Trigger overflow on DataWeight.
    {
        auto resources = CreateVector(5, 120, 0);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
        // DW and PDW doubled to 200, DSC stays at 10.
    }

    // Job 2: Verify DataSliceCount limit unchanged.
    {
        // Try to exceed hysteresis limit (15).
        auto resources = CreateVector(16, 0, 0);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);  // Should overflow - DSC hysteresis limit is 15.
    }

    // Job 3: Verify DataWeight limit was doubled.
    {
        auto resources = CreateVector(0, 180, 0);
        ASSERT_FALSE(tracker->CheckOverflow(resources));  // Fits in doubled limit.
    }

    // Job 4: Verify CompressedDataSize limit was also doubled.
    {
        auto resources = CreateVector(0, 0, 180);
        ASSERT_FALSE(tracker->CheckOverflow(resources));  // Fits in doubled limit.
    }
}

TEST_F(TJobSizeTrackerTest, LimitProgressionRatio_0_5)
{
    // Test geometric limit progression with ratio 0.5 (halves limits on overflow).
    // Vector format: [DW][PDW]
    // +--------------------------------------+
    // |                Limit                 |
    // +------------+------------+------------+
    // | HystLocal  | Cumulative |   Local    |
    // +------------+------------+------------+
    // | [inf][150] | [100][100] | [100][100] |
    // +------------+------------+------------+
    // | [inf][ 75] | [150][150] | [ 50][ 50] |
    // |            |            | x0.5 appl. |
    // +------------+------------+------------+
    // | [inf][ 37] | [175][175] | [ 25][ 25] |
    // |            |            | x0.5 appl. |
    // +------------+------------+------------+
    // | [inf][ 37] | [200][200] | [ 25][ 25] |
    // |            |            |  no x0.5   |
    // +------------+------------+------------+

    TJobSizeTrackerOptions options;
    options.LimitProgressionRatio = 0.5;
    options.LimitProgressionLength = 2;
    options.GeometricResources = {EResourceKind::DataWeight, EResourceKind::PrimaryDataWeight};

    auto tracker = CreateJobSizeTracker(
        /*limitVector*/ CreateDataWeightVector(100, 100),
        options,
        Logger);

    ASSERT_TRUE(tracker->CheckOverflow(CreateDataWeightVector(0, 101)));
    ASSERT_TRUE(tracker->CheckOverflow(CreateDataWeightVector(101, 0)));

    {
        auto resources = CreateDataWeightVector(101, 101);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
    }

    ASSERT_TRUE(tracker->CheckOverflow(CreateDataWeightVector(0, 76)));
    ASSERT_FALSE(tracker->CheckOverflow(CreateDataWeightVector(0, 75)));

    ASSERT_TRUE(tracker->CheckOverflow(CreateDataWeightVector(151, 0)));
    ASSERT_FALSE(tracker->CheckOverflow(CreateDataWeightVector(150, 0)));

    {
        auto resources = CreateDataWeightVector(151, 0);
        auto token = tracker->CheckOverflow(resources);
        ASSERT_TRUE(token);
        tracker->Flush(token);
    }

    ASSERT_TRUE(tracker->CheckOverflow(CreateDataWeightVector(0, 76)));
    ASSERT_FALSE(tracker->CheckOverflow(CreateDataWeightVector(0, 75)));

    ASSERT_TRUE(tracker->CheckOverflow(CreateDataWeightVector(201, 0)));
    ASSERT_FALSE(tracker->CheckOverflow(CreateDataWeightVector(200, 0)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools
