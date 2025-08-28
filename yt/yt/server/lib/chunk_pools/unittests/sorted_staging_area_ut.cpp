#include "chunk_pools_helpers.h"
#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/new_job_manager.h>
#include <yt/yt/server/lib/chunk_pools/sorted_staging_area.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NChunkPools {
namespace {

using namespace NChunkClient;
using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TSortedStagingAreaTest
    : public TSortedChunkPoolTestBase
{
protected:
    TRowBufferPtr RowBuffer_;
    TLogger Logger_;
    TComparator PrimaryComparator_;
    TComparator ForeignComparator_;
    std::unique_ptr<ISortedStagingArea> StagingArea_;

    struct TSliceExpectation
    {
        TKeyBound LowerBound;
        TKeyBound UpperBound;
        int ChunkId = 0;
    };

    struct TJobExpectations
    {
        i64 RowCount = 0;
        i64 DataWeight = 0;
        std::vector<TSliceExpectation> SliceExpectations;
    };

    struct TFlushExpectations
    {
        int ExpectedJobCount = 0;
        i64 ExpectedDataSliceCount = 0;
    };

    void SetUp() override
    {
        RowBuffer_ = New<TRowBuffer>();
        Logger_ = GetTestLogger();
        PrimaryComparator_ = TComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
        ForeignComparator_ = TComparator(std::vector<ESortOrder>(1, ESortOrder::Ascending));
    }

    TInputChunkPtr CreateChunk(i64 chunkId, i64 dataWeight = 1000, i64 rowCount = 100)
    {
        auto chunk = New<TInputChunk>();
        chunk->SetChunkId(TChunkId(chunkId, 0));
        chunk->SetTotalDataWeight(dataWeight);
        chunk->SetTotalRowCount(rowCount);
        chunk->SetTableIndex(0);
        return chunk;
    }

    TLegacyDataSlicePtr CreateDataSlice(
        TInputChunkPtr chunk,
        TKeyBound lowerBound = TKeyBound::MakeEmpty(false),
        TKeyBound upperBound = TKeyBound::MakeEmpty(true),
        int tableIndex = 0)
    {
        auto chunkSlice = CreateInputChunkSlice(chunk);

        chunkSlice->TransformToNewKeyless();

        auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
        dataSlice->SetInputStreamIndex(tableIndex);
        dataSlice->Tag = tableIndex;

        dataSlice->LowerLimit().KeyBound = lowerBound;
        dataSlice->UpperLimit().KeyBound = upperBound;

        dataSlice->ChunkSlices[0]->LowerLimit().KeyBound = lowerBound;
        dataSlice->ChunkSlices[0]->UpperLimit().KeyBound = upperBound;

        return dataSlice;
    }

    void CreateStagingArea(bool enableKeyGuarantee)
    {
        StagingArea_ = CreateSortedStagingArea(
            enableKeyGuarantee,
            PrimaryComparator_,
            ForeignComparator_,
            RowBuffer_,
            Logger_);
    }

    void FlushAndValidateStatistics(const TFlushExpectations& expectations)
    {
        auto statistics = StagingArea_->Flush();
        EXPECT_EQ(statistics.JobCount, expectations.ExpectedJobCount);
        EXPECT_EQ(statistics.DataSliceCount, expectations.ExpectedDataSliceCount);
    }

    void FlushExpectingNoOp()
    {
        FlushAndValidateStatistics({.ExpectedJobCount = 0, .ExpectedDataSliceCount = 0});
    }

    std::vector<TNewJobStub> FinishAndValidateStatistics(const TFlushExpectations& expectations)
    {
        auto [preparedJobs, finalStatistics] = std::move(*StagingArea_).Finish();

        EXPECT_EQ(std::ssize(preparedJobs), expectations.ExpectedJobCount);
        EXPECT_EQ(finalStatistics.JobCount, expectations.ExpectedJobCount);
        EXPECT_EQ(finalStatistics.DataSliceCount, expectations.ExpectedDataSliceCount);

        return std::move(preparedJobs);
    }

    void ValidateJob(
        TNewJobStub& job,
        const TJobExpectations& expectations)
    {
        job.Finalize();

        EXPECT_EQ(job.GetRowCount(), expectations.RowCount);
        EXPECT_EQ(job.GetSliceCount(), std::ssize(expectations.SliceExpectations));
        EXPECT_EQ(job.GetDataWeight(), expectations.DataWeight);

        const auto& stripes = job.GetStripeList()->Stripes;
        ASSERT_EQ(std::ssize(stripes), 1);

        const auto& dataSlices = stripes[0]->DataSlices;
        ASSERT_EQ(std::ssize(dataSlices), std::ssize(expectations.SliceExpectations));

        for (int i = 0; i < std::ssize(expectations.SliceExpectations); ++i) {
            YT_VERIFY(std::ssize(dataSlices[i]->ChunkSlices) == 1);
            ASSERT_EQ(dataSlices[i]->ChunkSlices[0]->GetInputChunk()->GetChunkId(), TChunkId(expectations.SliceExpectations[i].ChunkId, 0));
            ASSERT_EQ(dataSlices[i]->LowerLimit().KeyBound, expectations.SliceExpectations[i].LowerBound);
            ASSERT_EQ(dataSlices[i]->UpperLimit().KeyBound, expectations.SliceExpectations[i].UpperBound);
        }
    }

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
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedStagingAreaTest, SingleJobWithMultipleSlices)
{
    /**
     *  KEYS:  -inf | 0 | 1 | 2 | 3 | 4 | 5 | +inf
     * ================================================
     *                    SLICES
     *  S1:           [-------------------)
     *  S2:           [-------]
     * ================================================
     *                  PROMOTIONS
     *  P0:  <---)
     *  P1:  <--------)
     *  P2:           [-------------------------] flush
     * ================================================
     *                     JOBS
     *  J1:           [-------------------)
     */

    CreateStagingArea(/*enableKeyGuarantee*/ true); // [P0]

    StagingArea_->PromoteUpperBound(BuildBound("<", {0})); // [P1]

    StagingArea_->Put(CreateDataSlice( // [S1]
        CreateChunk(1),
        BuildBound(">=", {0}),
        BuildBound("<", {5})),
        ESliceType::Buffer);

    StagingArea_->Put(CreateDataSlice( // [S2]
        CreateChunk(2),
        BuildBound(">=", {0}),
        BuildBound("<=", {2})),
        ESliceType::Buffer);

    // Attempt flush before full promotion - should be no-op.
    FlushExpectingNoOp();

    auto preparedJobs = FinishAndValidateStatistics({.ExpectedJobCount = 1, .ExpectedDataSliceCount = 2}); // [P2 + flush]

    // [J1]: Verify job contains both slices.
    ValidateJob(preparedJobs.front(), {
        .RowCount = 200,
        .DataWeight = 2000,
        .SliceExpectations = {
            {BuildBound(">=", {0}), BuildBound("<", {5}), /*chunkId*/ 1},  // S1 bounds.
            {BuildBound(">=", {0}), BuildBound("<=", {2}), /*chunkId*/ 2},  // S2 bounds.
        },
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedStagingAreaTest, TwoPromotions)
{
    /**
     *  KEYS:  -inf | 0 | 1 | 2 | 3 | 4 | 5 | +inf
     * ================================================
     *                    SLICES
     *  S1:           [-------------------)
     *  S2:           [-----------]
     * ================================================
     *                  PROMOTIONS
     *  P0:  <---)
     *  P1:  <--------)
     *  P2:           [-----------] flush
     *  P3:                       (-------------] flush
     * ================================================
     *                     JOBS
     *  J1:           [-----------]
     *  J2:                       (-------)
     */

    CreateStagingArea(/*enableKeyGuarantee*/ true); // [P0]

    StagingArea_->PromoteUpperBound(BuildBound("<", {0})); // [P1]

    StagingArea_->Put(CreateDataSlice( // [S1]
        CreateChunk(1),
        BuildBound(">=", {0}),
        BuildBound("<", {5})),
        ESliceType::Buffer);

    StagingArea_->Put(CreateDataSlice( // [S2]
        CreateChunk(2),
        BuildBound(">=", {0}),
        BuildBound("<=", {3})),
        ESliceType::Buffer);

    StagingArea_->PromoteUpperBound(BuildBound("<=", {3})); // [P2 + flush]
    FlushAndValidateStatistics({.ExpectedJobCount = 1, .ExpectedDataSliceCount = 2});

    auto preparedJobs = FinishAndValidateStatistics({.ExpectedJobCount = 2, .ExpectedDataSliceCount = 3}); // [P3 + flush]

    // [J1]: [0, 3] - contains both S1 and S2 up to key 3.
    ValidateJob(preparedJobs[0], {
        .RowCount = 200,
        .DataWeight = 2000,
        .SliceExpectations = {
            {BuildBound(">=", {0}), BuildBound("<=", {3}), /*chunkId*/ 1},  // S1 portion in J1.
            {BuildBound(">=", {0}), BuildBound("<=", {3}), /*chunkId*/ 2},   // S2 in J1.
        },
    });

    // [J2]: (3, 5) - contains remainder of S1.
    ValidateJob(preparedJobs[1], {
        .RowCount = 100,
        .DataWeight = 1000,
        .SliceExpectations = {
            {BuildBound(">", {3}), BuildBound("<", {5}), /*chunkId*/ 1},  // S1 portion in J2.
        },
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedStagingAreaTest, MultipleJobsOverSingleSlice)
{
    /**
     *  KEYS:  -inf | 0 | 1 | 2 | 3 | 4 | 5 | +inf
     * ================================================
     *                    SLICES
     *  S1:           [-------------------]
     * ================================================
     *                  PROMOTIONS
     *  P0:  <---)
     *  P1:  <--------)
     *  P2:           [-------] flush
     *  P3:                   (---]
     *  P4:                   (-----------) flush
     *  P5:                               [----] flush
     * ================================================
     *                     JOBS
     *  J1:           [-------]
     *  J2:                   (-----------)
     *  J3:                               []
     */

    CreateStagingArea(/*enableKeyGuarantee*/ true); // [P0]

    StagingArea_->PromoteUpperBound(BuildBound("<", {0})); // [P1]

    StagingArea_->Put(CreateDataSlice( // [S1]
        CreateChunk(1),
        BuildBound(">=", {0}),
        BuildBound("<=", {5})),
        ESliceType::Buffer);

    StagingArea_->PromoteUpperBound(BuildBound("<=", {2})); // [P2 + flush]
    FlushAndValidateStatistics({.ExpectedJobCount = 1, .ExpectedDataSliceCount = 1});

    StagingArea_->PromoteUpperBound(BuildBound("<=", {3})); // [P3]

    StagingArea_->PromoteUpperBound(BuildBound("<", {5})); // [P4 + flush]
    FlushAndValidateStatistics({.ExpectedJobCount = 2, .ExpectedDataSliceCount = 2});

    auto preparedJobs = FinishAndValidateStatistics({.ExpectedJobCount = 3, .ExpectedDataSliceCount = 3}); // [P5 + flush]

    // [J1]: [0, 2]
    ValidateJob(preparedJobs[0], {
        .RowCount = 100,
        .DataWeight = 1000,
        .SliceExpectations = {
            {BuildBound(">=", {0}), BuildBound("<=", {2}), /*chunkId*/ 1},
        },
    });

    // [J2]: (2, 5)
    ValidateJob(preparedJobs[1], {
        .RowCount = 100,
        .DataWeight = 1000,
        .SliceExpectations = {
            {BuildBound(">", {2}), BuildBound("<", {5}), /*chunkId*/ 1},
        },
    });

    // [J3]: [5, 5]
    ValidateJob(preparedJobs[2], {
        .RowCount = 100,
        .DataWeight = 1000,
        .SliceExpectations = {
            {BuildBound(">=", {5}), BuildBound("<=", {5}), /*chunkId*/ 1},
        },
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedStagingAreaTest, WithForeign)
{
    /**
     *  KEYS:  -inf | 0 | 1 | 2 | 3 | 4 | 5 | +inf
     * ================================================
     *                    SLICES
     *  S1:           [-------)
     *  F1:               [---------------]
     *  S2:                           [---)
     * ================================================
     *                  PROMOTIONS
     *  P0:  <---)
     *  P1:  <--------)
     *  P2:           [-------] flush
     *  P3:                   (---] flush
     *  P4:                       (---)
     *  P5:                       (-------------] flush
     * ================================================
     *                     JOBS
     *  J1:           [-------)
     *  J2:                           [---)
     */
    CreateStagingArea(/*enableKeyGuarantee*/ true);

    StagingArea_->PromoteUpperBound(BuildBound("<", {0}));

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(1),
        BuildBound(">=", {0}),
        BuildBound("<", {2})),
        ESliceType::Buffer);

    EXPECT_TRUE(StagingArea_->GetForeignResourceVector().IsZero());

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(2),
        BuildBound(">=", {1}),
        BuildBound("<=", {5})),
        ESliceType::Foreign);

    EXPECT_EQ(StagingArea_->GetForeignResourceVector(), CreateVector(1, 1000, 0));

    StagingArea_->PromoteUpperBound(BuildBound("<=", {2}));
    FlushAndValidateStatistics({.ExpectedJobCount = 1, .ExpectedDataSliceCount = 2});

    EXPECT_EQ(StagingArea_->GetForeignResourceVector(), CreateVector(1, 1000, 0));

    StagingArea_->PromoteUpperBound(BuildBound("<=", {3}));
    FlushAndValidateStatistics({.ExpectedJobCount = 1, .ExpectedDataSliceCount = 2});

    EXPECT_EQ(StagingArea_->GetForeignResourceVector(), CreateVector(1, 1000, 0));

    StagingArea_->PromoteUpperBound(BuildBound("<", {4}));

    EXPECT_EQ(StagingArea_->GetForeignResourceVector(), CreateVector(1, 1000, 0));

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(3),
        BuildBound(">=", {4}),
        BuildBound("<", {5})),
        ESliceType::Buffer);

    auto preparedJobs = FinishAndValidateStatistics({.ExpectedJobCount = 2, .ExpectedDataSliceCount = 4});

    ValidateJob(preparedJobs[0], {
        .RowCount = 200,
        .DataWeight = 2000,
        .SliceExpectations = {
            {BuildBound(">=", {0}), BuildBound("<", {2}), /*chunkId*/ 1},
            {BuildBound(">=", {1}), BuildBound("<", {2}), /*chunkId*/ 2},
        },
    });

    ValidateJob(preparedJobs[1], {
        .RowCount = 200,
        .DataWeight = 2000,
        .SliceExpectations = {
            {BuildBound(">=", {4}), BuildBound("<", {5}), /*chunkId*/ 3},
            {BuildBound(">=", {4}), BuildBound("<", {5}), /*chunkId*/ 2},
        },
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedStagingAreaTest, ForeignResourceVectorTracksAddAndTrimOnFlush)
{
    // Prepare staging area with primary bound at (< 0].
    CreateStagingArea(/*enableKeyGuarantee*/ true);
    StagingArea_->PromoteUpperBound(BuildBound("<", {0}));

    // Initially zero.
    EXPECT_TRUE(StagingArea_->GetForeignResourceVector().IsZero());

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(/*chunkId*/ 11, /*dataWeight*/ 700),
        BuildBound(">=", {-10}),
        BuildBound("<=", {-5})),
        ESliceType::Foreign); // F1

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(/*chunkId*/ 12, /*dataWeight*/ 800),
        BuildBound(">=", {-3}),
        BuildBound("<", {0})),
        ESliceType::Foreign); // F2

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(/*chunkId*/ 13, /*dataWeight*/ 500),
        BuildBound(">=", {-1}),
        BuildBound("<=", {0})),
        ESliceType::Foreign); // F_touch

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(/*chunkId*/ 14, /*dataWeight*/ 600),
        BuildBound(">=", {1}),
        BuildBound("<=", {5})),
        ESliceType::Foreign); // F_right

    // Verify aggregated foreign resources: 4 slices, total foreign weight = 700+800+500+600 = 2600.
    EXPECT_EQ(StagingArea_->GetForeignResourceVector(), CreateVector(/*count*/ 4, /*dataWeight*/ 2600, /*primaryDataWeight*/ 0));

    // Add a primary slice so that a job is built on flush with lower bound at >= 0.
    StagingArea_->Put(CreateDataSlice(
        CreateChunk(/*chunkId*/ 21, /*dataWeight*/ 1000),
        BuildBound(">=", {0}),
        BuildBound("<", {5})),
        ESliceType::Buffer);

    // Move buffer to main, set upper bound, and flush. This triggers trimming of foreign slices,
    // whose upper bound is entirely to the left of the job lower bound (>= 0): F1 and F2.
    StagingArea_->PromoteUpperBound(BuildBound("<=", {2}));
    FlushAndValidateStatistics({.ExpectedJobCount = 1, .ExpectedDataSliceCount = 3});

    // Remaining foreign slices: F_touch (upper=0 inclusive) and F_right. Total weight = 500 + 600 = 1100.
    EXPECT_EQ(StagingArea_->GetForeignResourceVector(), CreateVector(/*count*/ 2, /*dataWeight*/ 1100, /*primaryDataWeight*/ 0));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedStagingAreaTest, SolidsOnly)
{
    /**
     *  KEYS:  -inf | 0 | 1 | 2 | 3 | 4 | 5 | +inf
     * ================================================
     *                    SLICES
     *  SLD1:         (=======)
     *  SLD2:         (===========]
     *  SLD3:                 [===========]
     *  SLD4:                 [=======)
     * ================================================
     *                     JOBS
     *  J1:           (=======)
     *  J2:           (===========]
     *  J2:                   [===========]
     */
    CreateStagingArea(/*enableKeyGuarantee*/ false);

    StagingArea_->PromoteUpperBound(BuildBound("<=", {0}));
    FlushExpectingNoOp();

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(1),
        BuildBound(">", {0}),
        BuildBound("<", {2})),
        ESliceType::Solid);

    FlushAndValidateStatistics({.ExpectedJobCount = 1, .ExpectedDataSliceCount = 1});

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(2),
        BuildBound(">", {0}),
        BuildBound("<=", {3})),
        ESliceType::Solid);

    FlushAndValidateStatistics({.ExpectedJobCount = 2, .ExpectedDataSliceCount = 2});

    StagingArea_->PromoteUpperBound(BuildBound("<", {2}));

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(3),
        BuildBound(">=", {2}),
        BuildBound("<=", {5})),
        ESliceType::Solid);

    StagingArea_->Put(CreateDataSlice(
        CreateChunk(4),
        BuildBound(">=", {2}),
        BuildBound("<", {4})),
        ESliceType::Solid);

    auto preparedJobs = FinishAndValidateStatistics({.ExpectedJobCount = 3, .ExpectedDataSliceCount = 4});

    ValidateJob(preparedJobs[0], {
        .RowCount = 100,
        .DataWeight = 1000,
        .SliceExpectations = {
            {BuildBound(">", {0}), BuildBound("<", {2}), /*chunkId*/ 1},
        },
    });

    ValidateJob(preparedJobs[1], {
        .RowCount = 100,
        .DataWeight = 1000,
        .SliceExpectations = {
            {BuildBound(">", {0}), BuildBound("<=", {3}), /*chunkId*/ 2},
        },
    });

    ValidateJob(preparedJobs[2], {
        .RowCount = 200,
        .DataWeight = 2000,
        .SliceExpectations = {
            {BuildBound(">=", {2}), BuildBound("<=", {5}), /*chunkId*/ 3},
            {BuildBound(">=", {2}), BuildBound("<", {4}), /*chunkId*/ 4},
        },
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools
