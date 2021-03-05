#include "new_sorted_job_builder.h"

#include "helpers.h"
#include "input_stream.h"
#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/heap.h>

#include <cmath>

namespace NYT::NChunkPools {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

//! Helper structure for representing job parameters.
struct TAggregatedStatistics
{
    i64 DataSliceCount = 0;
    i64 DataWeight = 0;
    i64 PrimaryDataWeight = 0;

    static TAggregatedStatistics FromDataSlice(const TLegacyDataSlicePtr& dataSlice, bool isPrimary)
    {
        return {
            .DataSliceCount = 1,
            .DataWeight = dataSlice->GetDataWeight(),
            .PrimaryDataWeight = isPrimary ? dataSlice->GetDataWeight() : 0
        };
    }

    TAggregatedStatistics operator+(const TAggregatedStatistics& other) const
    {
        return {
            .DataSliceCount = DataSliceCount + other.DataSliceCount,
            .DataWeight = DataWeight + other.DataWeight,
            .PrimaryDataWeight = PrimaryDataWeight + other.PrimaryDataWeight
        };
    }

    TAggregatedStatistics& operator+=(const TAggregatedStatistics& other)
    {
        DataSliceCount += other.DataSliceCount;
        DataWeight += other.DataWeight;
        PrimaryDataWeight += other.PrimaryDataWeight;
        return *this;
    }

    TAggregatedStatistics& operator-=(const TAggregatedStatistics& other)
    {
        DataSliceCount -= other.DataSliceCount;
        DataWeight -= other.DataWeight;
        PrimaryDataWeight -= other.PrimaryDataWeight;
        return *this;
    }

    bool operator <=(const TAggregatedStatistics& other) const
    {
        return
            DataSliceCount <= other.DataSliceCount &&
            DataWeight <= other.DataWeight &&
            PrimaryDataWeight <= other.PrimaryDataWeight;
    }

    bool operator >=(const TAggregatedStatistics& other) const
    {
        return
            DataSliceCount >= other.DataSliceCount ||
            DataWeight >= other.DataWeight ||
            PrimaryDataWeight >= other.PrimaryDataWeight;
    }

    bool operator >(const TAggregatedStatistics& other) const
    {
        return
            DataSliceCount > other.DataSliceCount ||
            DataWeight > other.DataWeight ||
            PrimaryDataWeight > other.PrimaryDataWeight;
    }

    bool IsZero() const
    {
        return DataSliceCount == 0 && DataWeight == 0 && PrimaryDataWeight == 0;
    }
};

TString ToString(const TAggregatedStatistics& statistics)
{
    return Format("{DSC: %v, DW: %v, PDW: %v}", statistics.DataSliceCount, statistics.DataWeight, statistics.PrimaryDataWeight);
}

////////////////////////////////////////////////////////////////////////////////

TString GetDataSliceDebugString(const TLegacyDataSlicePtr& dataSlice)
{
    std::vector<TChunkId> chunkIds;
    chunkIds.reserve(dataSlice->ChunkSlices.size());
    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
        chunkIds.push_back(chunkSlice->GetInputChunk()->GetChunkId());
    }
    return Format("{Address: %v, DataWeight: %v, KeyBounds: %v:%v, InputStreamIndex: %v, ChunkIds: %v}",
        dataSlice.Get(),
        dataSlice->GetDataWeight(),
        dataSlice->LowerLimit().KeyBound,
        dataSlice->UpperLimit().KeyBound,
        dataSlice->InputStreamIndex,
        chunkIds);
}

////////////////////////////////////////////////////////////////////////////////

// == WHAT IS THIS? ==
//
// Here goes the class that serves for means of job creation. Think of it as of a staging
// area for data slices; it reacts on events like "promote current job upper bound to the
// next interesting endpoint" of "(forcefully) flush".
//
// All data slices in the staging area are divided into four domains.
//
// === Main ===
//
// Contains data slices such that are going in the next job to be built. On flush they are
// cut using current upper bound into two halves, first of which goes to the job, while
// second goes to the BufferNonSingleton.
//
// Invariant: for all main data slices D condition D.lowerBound < UpperBound_ holds
// with the only exception of singleton data slices, for which it may happen
// that D.LowerBound == UpperBound_ (i.e. singleton key is located exactly
// to the right of the ray defined by UpperBound_). Note that the latter
// case may happen only when UpperBound_ is exclusive.
//
// === BufferNonSingleton ===
//
// Contains data slices that appeared at the same time upper bound took its current place.
//
// Invariants: 1) for all buffer data slices D holds D.LowerBound == UpperBound_.Invert().
// 2) if key guarantee is disabled, contains only non-singleton data slices.
//
// === BufferSingleton ===
//
// Similar to the previous one, but appears only when key guarantee is disabled and contains
// only singleton data slices.
//
// === Foreign ===
//
// Contains foreign data slices. They are stored in a priority queue ordered by slice's
// upper bound. Such order allows us to trim foreign data slices that are not relevant any more.
//
// == EXAMPLES ==
//
// 1) EnableKeyGuarantee = true, no foreign data is present (typical sorted reduce operation).
//
//                       exclusive
//                      upper bound
//      <Main>               )               <BufferNonSingleton>
//                           )
// A:                        )[-------]
// B:              [---------)
// C:                        )[]
//                           )
// D:           [------------)---)
// E:    (---------]         )
//                           )
// --------------------------)--------------------------------------> keys
//
// Slices B, D and E are in Main domain, slices A and C are in BufferNonSingleton domain.
// Slice C is a single-key slice, but we treat it as a regular BufferNonSingleton slice
// since key guarantee is enabled.
// Slice D spans across current upper bound.
// If Flush() is to be called now, D will be cut into two parts, and job will be formed
// of E, B and D's left part.
//
// 2) EnableKeyGuarantee = false, no foreign data is present.
//
//                       exclusive
//                      upper bound
//      <Main>               )
//                           )
// A:                        )[-------]      <-- <BufferNonSingleton>
// B:              [---------)
// C1:                       )[]             <\
// C2:                       )[]             < - <BufferSingleton>
// C3:                       )[]             </
//                           )
// D:           [------------)---]
// E:    [---------]         )
//                           )
// --------------------------)--------------------------------------> keys
//
// Same as previous, but key guarantee is disabled. In such circumstances,
// slices C1-3 have special meaning for us: they may be attached to the current job
// (despite the fact they do not belong to the current key bound).
//
// Moreover, they are allowed to be sliced by rows in situation when taking whole slice
// violates job limits. In such case, left part of the slice goes to the job,
// while right part resides in the BufferSingleton domain, after which it will
// be considered for including into the next job.
//
// Note that in such case first job contains all slices from current Main domain,
// while second, third, ... jobs will contain only singleton slices.
//
// 3) EnableKeyGuarantee = true, foreign data is present.
//                       inclusive
//                      upper bound
//      <Main>               ]               <BufferNonSingleton>
//                           ]
// A:                        ](-------]
// B:              [---------]
// C:                       []
//                           ]
// D:           [------------]---)
// E:    (---------]         ]
//                           ]
// --------------------------]--------------------------------------> keys
//                           ]
// F:  [-------------]       ]                 <Foreign>
// G:                     [--]------]
//                           ]
//
// In this case foreign data is present. After we call Flush(), all primary slices
// from Main domain disappear making slice F irrelevant, so it is going to be trimmed
// off Foreign domain.
//
// Also this case illustrates that upper bound may be inclusive (e.g. when it is induced
// by an inclusive lower bound of a primary slice A), but this does not actually affect
// any logic.

DEFINE_ENUM(EDomainKind,
    (Main)
    (BufferSingleton)
    (BufferNonSingleton)
    (Foreign)
)

//! This class is responsible for holding the current "working set" of data slices.
class TStagingArea
{
public:
    TStagingArea(
        bool enableKeyGuarantee,
        TComparator primaryComparator,
        TComparator foreignComparator,
        const TRowBufferPtr& rowBuffer,
        TAggregatedStatistics limitStatistics,
        i64 maxTotalDataSliceCount,
        i64 inputSliceDataWeight,
        const TInputStreamDirectory& inputStreamDirectory,
        bool logDetails,
        const TLogger& logger)
        : EnableKeyGuarantee_(enableKeyGuarantee)
        , PrimaryComparator_(primaryComparator)
        , ForeignComparator_(foreignComparator)
        , LimitStatistics_(limitStatistics)
        , MaxTotalDataSliceCount_(maxTotalDataSliceCount)
        , InputSliceDataWeight_(inputSliceDataWeight)
        , LogDetails_(logDetails)
        , RowBuffer_(rowBuffer)
        , InputStreamDirectory_(inputStreamDirectory)
        , Logger(logger)
        , ForeignDomain_(ForeignComparator_)
    {
        YT_LOG_DEBUG_IF(LogDetails_, "Staging area instantiated (LimitStatistics: %v)", LimitStatistics_);
    }

    //! Promote upper bound for currently built job.
    void PromoteUpperBound(TKeyBound upperBound)
    {
        YT_LOG_DEBUG_IF(LogDetails_, "Upper bound promoted (UpperBound: %v)", upperBound);

        // NB: The leftmost endpoint may be >=[] when dealing with sorted dynamic stores,
        // and it is the only case when UpperBound_ may not be smaller than upperBound.
        YT_VERIFY(
            PrimaryComparator_.CompareKeyBounds(UpperBound_, upperBound) < 0 ||
            (PrimaryComparator_.CompareKeyBounds(UpperBound_, upperBound) == 0 &&
            upperBound.IsEmpty()));

        UpperBound_ = upperBound;

        // Buffer slices are not attached to current upper bound any more, so they
        // should me moved to the main area.
        TransferWholeBufferToMain();
    }

    //! Put new data slice. It must be true that dataSlice.LowerBound == UpperBound_.Invert().
    void Put(const TLegacyDataSlicePtr& dataSlice, bool isPrimary)
    {
        YT_VERIFY(dataSlice->Tag);
        YT_VERIFY(dataSlice->LowerLimit().KeyBound == UpperBound_.Invert());

        if (!isPrimary) {
            PutToDomain(EDomainKind::Foreign, dataSlice);
        } else if (
            !EnableKeyGuarantee_ &&
            PrimaryComparator_.TryAsSingletonKey(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound) &&
            // NB: versioned data slices can't be sliced by rows.
            !InputStreamDirectory_.GetDescriptor(*dataSlice->Tag).IsVersioned())
        {
            PutToDomain(EDomainKind::BufferSingleton, dataSlice);
        } else {
            PutToDomain(EDomainKind::BufferNonSingleton, dataSlice);
        }
    }

    //! Barriers are used to indicate positions which should not be overlapped by jobs
    //! (in particular, pivot keys and teleport chunks define barriers).
    void PutBarrier()
    {
        auto& job = PreparedJobs_.emplace_back();
        job.SetIsBarrier(true);
    }

    //! Either try flushing or forcefully flush data slices into one or more new jobs.
    //! Non-force version should be called after each introduction of new portion of data slices;
    //! force version is called whenever pivot keys or teleport chunks are reached.
    void Flush(bool force)
    {
        // If we have no Main nor BufferSingleton slices, we have nothing to do.
        if (IsExhausted()) {
            // Nothing to flush.
            return;
        }

        // In order to flush, we should be forcefully asked to or we should have
        // enough data for at least one job.
        if (!force && !IsOverflow()) {
            return;
        }

        YT_LOG_DEBUG_IF(
            LogDetails_,
            "Performing flush (Statistics: %v, Limits: %v, IsOverflow: %v, Force: %v)",
            GetStatisticsDebugString(),
            LimitStatistics_,
            IsOverflow(),
            force);

        // By this moment singleton slices are not yet in the Main, so we cut only
        // proper Main data slices.
        CutMainByUpperBound();

        bool progressMade;
        do
        {
            // Flag indicating if we were able to form a non-trivial job.
            progressMade = false;

            // First, try to fill current job with singleton slices.
            if (!EnableKeyGuarantee_) {
                progressMade |= TryTransferSingletonsToMain(force);
            }
            // By this moment some part of singleton jobs could have been added
            // to main. Now try flushing Main domain into job.
            progressMade |= TryFlushMain();
        } while (progressMade);

        YT_LOG_DEBUG_IF(
            LogDetails_,
            "Flush finished (Statistics: %v)",
            GetStatisticsDebugString());

        // If we were explicitly asked to forcefully flush, make a sanity check
        // that Main and BufferSingleton domains are empty.
        if (force) {
            for (const auto& domainKind : {EDomainKind::Main, EDomainKind::BufferSingleton}) {
                const auto& domain = PrimaryDomains_[domainKind];
                YT_VERIFY(domain.DataSlices.empty());
                YT_VERIFY(domain.Statistics.IsZero());
            }
        }
    }

    //! Called at the end of processing to flush all remaining data slices into jobs.
    void Finish()
    {
        YT_LOG_DEBUG_IF(LogDetails_, "Finishing work in staging area");

        PromoteUpperBound(TKeyBound::MakeUniversal(/* isUpper */ true));

        Flush(/* force */ true);
        for (const auto& domain : PrimaryDomains_) {
            YT_VERIFY(domain.DataSlices.empty());
            YT_VERIFY(domain.Statistics.IsZero());
        }
    }

    std::vector<TNewJobStub>& PreparedJobs()
    {
        return PreparedJobs_;
    }

    //! Total number of data slices in all created jobs.
    //! Used for internal bookkeeping by the outer code.
    i64 GetTotalDataSliceCount() const
    {
        return TotalDataSliceCount_;
    }

private:
    bool EnableKeyGuarantee_;
    TComparator PrimaryComparator_;
    TComparator ForeignComparator_;
    TAggregatedStatistics LimitStatistics_;
    i64 MaxTotalDataSliceCount_;
    i64 InputSliceDataWeight_;
    bool LogDetails_;
    TRowBufferPtr RowBuffer_;
    TInputStreamDirectory InputStreamDirectory_;
    TLogger Logger;

    //! Upper bound using which all data slices in Main domain are to be cut.
    //! NB: actual upper bound of job to be built may differ from #UpperBound_
    //! in case when singleton data slices are added to the job; in this case
    //! actual upper bound for a job will be #UpperBound_.ToggleInclusiveness()
    //! (i.e. exclusive instead of inclusive).
    TKeyBound UpperBound_ = TKeyBound::MakeEmpty(/* isUpper */ true);

    i64 TotalDataSliceCount_ = 0;
    std::vector<TNewJobStub> PreparedJobs_;

    //! These flags are used only for internal sanity check.
    bool PreviousJobContainedSingleton_ = false;
    bool CurrentJobContainsSingleton_ = false;

    //! Previous job upper bound, used for internal sanity check.
    TKeyBound PreviousJobUpperBound_ = TKeyBound::MakeEmpty(/* isUpper */ true);

    //! Structure holding data slices for one of primary domains with their aggregated statistics.
    struct TPrimaryDomain
    {
        TAggregatedStatistics Statistics;
        TRingQueue<TLegacyDataSlicePtr> DataSlices = TRingQueue<TLegacyDataSlicePtr>();

        void AddDataSlice(TLegacyDataSlicePtr dataSlice)
        {
            Statistics += TAggregatedStatistics::FromDataSlice(dataSlice, /* isPrimary */ true);
            DataSlices.push(std::move(dataSlice));
        }

        void Clear()
        {
            Statistics = TAggregatedStatistics();
            DataSlices.clear();
        }
    };

    //! Similar to previous, but for foreign data slices.
    struct TForeignDomain
    {
        TAggregatedStatistics Statistics;

        // TODO(max42): YT-14357.
        //! Priority queue of data slices using upper key bound as priority.
        std::vector<TLegacyDataSlicePtr> DataSlices;
        std::function<bool(const TLegacyDataSlicePtr&, const TLegacyDataSlicePtr&)> DataSliceUpperBoundComparator;

        TForeignDomain(const TComparator& foreignComparator)
            : DataSliceUpperBoundComparator([&foreignComparator] (const auto& lhs, const auto& rhs) {
                return foreignComparator.CompareKeyBounds(lhs->UpperLimit().KeyBound, rhs->UpperLimit().KeyBound) < 0;
            })
        { }

        void AddDataSlice(TLegacyDataSlicePtr dataSlice)
        {
            Statistics += TAggregatedStatistics::FromDataSlice(dataSlice, /* isPrimary */ false);
            DataSlices.emplace_back(std::move(dataSlice));
            AdjustHeapBack(DataSlices.begin(), DataSlices.end(), DataSliceUpperBoundComparator);
        }
    };

    TEnumIndexedVector<EDomainKind, TPrimaryDomain> PrimaryDomains_;
    TForeignDomain ForeignDomain_;

    void PutToDomain(EDomainKind domain, const TLegacyDataSlicePtr& dataSlice)
    {
        bool isPrimary = domain != EDomainKind::Foreign;
        if (isPrimary) {
            PrimaryDomains_[domain].AddDataSlice(dataSlice);
        } else {
            ForeignDomain_.AddDataSlice(dataSlice);
        }
    }

    TAggregatedStatistics GetTotalStatistics() const
    {
        return
            PrimaryDomains_[EDomainKind::Main].Statistics +
            PrimaryDomains_[EDomainKind::BufferNonSingleton].Statistics +
            PrimaryDomains_[EDomainKind::BufferSingleton].Statistics +
            ForeignDomain_.Statistics;
    }

    TString GetStatisticsDebugString() const
    {
        std::vector<TString> parts;
        parts.emplace_back(Format("Main: %v", PrimaryDomains_[EDomainKind::Main].Statistics));
        parts.emplace_back(Format("BufferNonSingleton: %v", PrimaryDomains_[EDomainKind::BufferNonSingleton].Statistics));
        if (!EnableKeyGuarantee_) {
            parts.emplace_back(Format("BufferSingleton: %v", PrimaryDomains_[EDomainKind::BufferSingleton].Statistics));
        }
        return Format("{%v}", JoinToString(parts, AsStringBuf(", ")));
    }

    //! Check if it is time to build a job. Indeed, if we promote upper bound instead,
    //! on the next iteration we will get an overflow situated in Main domain, so it is
    //! better to flush now.
    bool IsOverflow() const
    {
        return GetTotalStatistics() > LimitStatistics_;
    }

    //! Check if we have at least one data slice to build job right now.
    bool IsExhausted() const
    {
        return PrimaryDomains_[EDomainKind::Main].Statistics.IsZero() && PrimaryDomains_[EDomainKind::BufferSingleton].Statistics.IsZero();
    }

    void CutMainByUpperBound()
    {
        YT_LOG_DEBUG_IF(LogDetails_, "Cutting main domain by upper bound (UpperBound: %v)", UpperBound_);

        auto& mainDataSlices = PrimaryDomains_[EDomainKind::Main].DataSlices;
        for (auto it = mainDataSlices.begin(); it != mainDataSlices.end(); mainDataSlices.move_forward(it)) {
            auto& dataSlice = *it;

            // Right part of the data slice goes to the BufferNonSingleton domain.
            auto restDataSlice = CreateInputDataSlice(dataSlice, PrimaryComparator_, UpperBound_.Invert(), dataSlice->UpperLimit().KeyBound);
            restDataSlice->LowerLimit().KeyBound = PrimaryComparator_.StrongerKeyBound(UpperBound_.Invert(), restDataSlice->LowerLimit().KeyBound);
            // It may happen that data slice is entirely inside current upper bound (e.g. slice E from example 1 above).
            if (!PrimaryComparator_.IsRangeEmpty(restDataSlice->LowerLimit().KeyBound, restDataSlice->UpperLimit().KeyBound)) {
                restDataSlice->CopyPayloadFrom(*dataSlice);
                PutToDomain(EDomainKind::BufferNonSingleton, restDataSlice);
            }

            // Left part of the data slice resides in the Main domain.
            dataSlice = CreateInputDataSlice(dataSlice, PrimaryComparator_, dataSlice->LowerLimit().KeyBound, UpperBound_);

            // Data slices are moved into Main domain strictly after they are first introduced (i.e. after promotion of upper bound),
            // so the left part can't be empty.
            YT_VERIFY(!PrimaryComparator_.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound));
        }
    }

    //! Try to transfer arbitrary number of whole data slices and at most one row-sliced data slice
    //! from BufferSingleton domain to the Main domain. If at least one data sliced is transferred,
    //! return true; otherwise return false.
    bool TryTransferSingletonsToMain(bool force)
    {
        auto& mainDomain = PrimaryDomains_[EDomainKind::Main];
        auto& singletonDomain = PrimaryDomains_[EDomainKind::BufferSingleton];

        while (true) {
            // Check if there is at least one data slices to transfer.
            if (singletonDomain.Statistics.IsZero()) {
                YT_LOG_DEBUG_IF(LogDetails_, "Singleton domain exhausted");
                return false;
            }

            // Stop process if we are not forced to transfer singletons up to the end
            // and if Main domain is already full.
            if (!force && mainDomain.Statistics >= LimitStatistics_) {
                YT_LOG_DEBUG_IF(
                    LogDetails_,
                    "Main domain saturated (Statistics: %v)", mainDomain.Statistics);
                return false;
            }

            auto& dataSlice = singletonDomain.DataSlices.front();

            // Check invariants for buffer singleton data slices.
            YT_VERIFY(dataSlice->LowerLimit().KeyBound == UpperBound_.Invert());
            YT_VERIFY(dataSlice->LowerLimit().KeyBound.IsInclusive);

            auto statistics = TAggregatedStatistics::FromDataSlice(dataSlice, /* isPrimary */ true);

            auto takeWhole = [&] {
                YT_LOG_DEBUG_IF(
                    LogDetails_,
                    "Adding whole singleton data slice to main domain (DataSlice: %v, Statistics: %v)",
                    GetDataSliceDebugString(dataSlice),
                    statistics);
                mainDomain.AddDataSlice(dataSlice);
                singletonDomain.Statistics -= statistics;
                singletonDomain.DataSlices.pop();
                CurrentJobContainsSingleton_ = true;
            };

            // Why would we want to take the whole slice? There are three cases.
            // 1) It may fit into the gap; 2) it may be small enough to be considered negligible
            // or 3) we have no other choice.
            if (mainDomain.Statistics + statistics <= LimitStatistics_ ||
                statistics.DataWeight <= InputSliceDataWeight_ ||
                force)
            {
                takeWhole();
            } else {
                auto gapStatistics = LimitStatistics_;
                gapStatistics -= mainDomain.Statistics;

                YT_LOG_DEBUG_IF(
                    LogDetails_,
                    "Trying to fill the gap (GapStatistics: %v, DataSlice: %v)",
                    gapStatistics,
                    GetDataSliceDebugString(dataSlice));

                // Ok, we know that this data slice is going to be the last we put into the main domain.
                // Let's calculate which fraction of the data slices we take for now.

                // First, estimate what is the maximum fraction that does not violate the remaining gap.
                auto fractionUpperBound = static_cast<double>(gapStatistics.DataWeight) / statistics.DataWeight;
                if (statistics.PrimaryDataWeight != 0) {
                    fractionUpperBound = std::min(
                        fractionUpperBound,
                        static_cast<double>(gapStatistics.PrimaryDataWeight) / statistics.PrimaryDataWeight);
                }

                // Second, taking smaller than InputSliceDataWeight_ is meaningless.
                auto sliceDataWeightFractionLowerBound = static_cast<double>(InputSliceDataWeight_) / statistics.DataWeight;

                auto fraction = fractionUpperBound;
                if (fraction < sliceDataWeightFractionLowerBound) {
                    fraction = sliceDataWeightFractionLowerBound;
                }

                // Finally, if we already took more than 90% of data slice, take it as a whole.
                constexpr double UpperFractionThreshold = 0.9;

                if (fraction >= UpperFractionThreshold) {
                    YT_LOG_DEBUG_IF(LogDetails_, "Fraction for the remaining data slice is high enough to take it as a whole (Fraction: %v)", fraction);
                    takeWhole();
                    YT_LOG_DEBUG_IF(
                        LogDetails_,
                        "Main domain saturated after transferring final whole data slice (Statistics: %v)", mainDomain.Statistics);
                } else {
                    // Divide slice in desired proportion using row indices.
                    auto lowerRowIndex = dataSlice->LowerLimit().RowIndex.value_or(0);
                    auto upperRowIndex = dataSlice->UpperLimit().RowIndex.value_or(dataSlice->GetSingleUnversionedChunkOrThrow()->GetRowCount());
                    YT_VERIFY(lowerRowIndex < upperRowIndex);
                    auto rowCount = static_cast<i64>(std::ceil((upperRowIndex - lowerRowIndex) * fraction));
                    rowCount = ClampVal<i64>(rowCount, 0, upperRowIndex - lowerRowIndex);

                    YT_LOG_DEBUG_IF(
                        LogDetails_,
                        "Splitting data slice by rows (Fraction: %v, LowerRowIndex: %v, UpperRowIndex: %v, RowCount: %v, MiddleRowIndex: %v)",
                        fraction,
                        lowerRowIndex,
                        upperRowIndex,
                        rowCount,
                        lowerRowIndex + rowCount);
                    auto [leftDataSlice, rightDataSlice] = dataSlice->SplitByRowIndex(rowCount);
                    // Discard the original singleton data slice.
                    singletonDomain.Statistics -= TAggregatedStatistics::FromDataSlice(dataSlice, /* isPrimary */ true);

                    if (rowCount == upperRowIndex - lowerRowIndex) {
                        // In some borderline cases this may happen... just discard this data slice.
                        singletonDomain.DataSlices.pop();
                    } else {
                        // Add right part to the singleton domain.
                        singletonDomain.Statistics += TAggregatedStatistics::FromDataSlice(rightDataSlice, /* isPrimary */ true);
                        dataSlice.Swap(rightDataSlice);
                    }

                    if (rowCount > 0) {
                        // Finally, add left part to the Main domain.
                        mainDomain.AddDataSlice(leftDataSlice);
                        CurrentJobContainsSingleton_ = true;
                        YT_LOG_DEBUG_IF(
                            LogDetails_,
                            "Main domain saturated after transferring final partial data slice (Statistics: %v)", mainDomain.Statistics);
                    }
                }

                return true;
            };
        }

        YT_ABORT();
    }

    void TransferWholeBufferToMain()
    {
        for (auto bufferDomain : {EDomainKind::BufferNonSingleton, EDomainKind::BufferSingleton}) {
            auto& domain = PrimaryDomains_[bufferDomain];
            for (auto it = domain.DataSlices.begin(); it != domain.DataSlices.end(); domain.DataSlices.move_forward(it)) {
                PrimaryDomains_[EDomainKind::Main].AddDataSlice(std::move(*it));
            }
            PrimaryDomains_[bufferDomain].Clear();
        }
    }

    void ValidateCurrentJobBounds(TKeyBound actualLowerBound, TKeyBound actualUpperBound) const
    {
        YT_LOG_DEBUG_IF(
            LogDetails_,
            "Current job key bounds (KeyBounds: %v:%v)",
            actualLowerBound,
            actualUpperBound);

        // In general case, previous and current job are located like this:
        //
        // C: --------------[-------------)-----
        // P: ----[---------)-------------------
        //
        // or like this:
        //
        // C: --------------(-------------)-----
        // P: ----[---------]-------------------
        //
        // But if the previous job contained singleton, it spanned a bit wider,
        // including one extra key (obtained from singleton slice). In this case
        // picture may look like the following:
        //
        // C: --------------[-------------]---
        // P: ----[---------]-----------------
        //
        // First, we assert that the previous job is located to the left from the
        // current one (possibly, with intersection consisting of a single key).

        if (PreviousJobContainedSingleton_) {
            YT_VERIFY(
                PrimaryComparator_.CompareKeyBounds(actualLowerBound, PreviousJobUpperBound_) >= 0 ||
                PrimaryComparator_.TryAsSingletonKey(actualLowerBound, PreviousJobUpperBound_));
        } else {
            YT_VERIFY(PrimaryComparator_.CompareKeyBounds(actualLowerBound, PreviousJobUpperBound_) >= 0);
        }

        // Second, assert that the whole job is located to the left of UpperBound_ with
        // the same exception of a job including the singleton key, in which case
        // upper bound is toggled.

        TKeyBound theoreticalUpperBound;
        if (CurrentJobContainsSingleton_) {
            YT_VERIFY(!UpperBound_.IsInclusive);
            theoreticalUpperBound = UpperBound_.ToggleInclusiveness();
        } else {
            theoreticalUpperBound = UpperBound_;
        }

        YT_VERIFY(PrimaryComparator_.CompareKeyBounds(actualUpperBound, theoreticalUpperBound) <= 0);
    }

    //! Trim leftmost foreign slices (in respect to their upper limits) until
    //! leftmost of them starts to intersect the lower bound of current job.
    void TrimForeignSlices(TKeyBound actualLowerBound)
    {
        while (!ForeignDomain_.DataSlices.empty() &&
            ForeignComparator_.IsRangeEmpty(actualLowerBound, ForeignDomain_.DataSlices.front()->UpperLimit().KeyBound))
        {
            YT_LOG_DEBUG_IF(LogDetails_, "Trimming foreign data slice (DataSlice: %v)", ForeignDomain_.DataSlices.front());
            ForeignDomain_.Statistics -= TAggregatedStatistics::FromDataSlice(ForeignDomain_.DataSlices.front(), /* isPrimary */ false);
            ExtractHeap(ForeignDomain_.DataSlices.begin(), ForeignDomain_.DataSlices.end(), ForeignDomain_.DataSliceUpperBoundComparator);
            ForeignDomain_.DataSlices.pop_back();
        }
    }

    //! If there is at least one data slice in the main domain, form a job and return true.
    //! Otherwise, return false.
    bool TryFlushMain()
    {
        if (PrimaryDomains_[EDomainKind::Main].Statistics.IsZero()) {
            YT_LOG_DEBUG_IF(LogDetails_, "Nothing to flush");
            return false;
        }

        auto& job = PreparedJobs_.emplace_back();

        auto& mainDataSlices = PrimaryDomains_[EDomainKind::Main].DataSlices;

        YT_LOG_DEBUG_IF(LogDetails_, "Flushing main domain into job (Statistics: %v)", PrimaryDomains_[EDomainKind::Main].Statistics);

        // Calculate the actual lower and upper bounds of newly formed job and move data slices to the job.
        auto actualLowerBound = TKeyBound::MakeEmpty(/* isUpper */ false);
        auto actualUpperBound = TKeyBound::MakeEmpty(/* isUpper */ true);
        for (auto it = mainDataSlices.begin(); it != mainDataSlices.end(); mainDataSlices.move_forward(it)) {
            const auto& dataSlice = *it;
            actualLowerBound = PrimaryComparator_.WeakerKeyBound(dataSlice->LowerLimit().KeyBound, actualLowerBound);
            actualUpperBound = PrimaryComparator_.WeakerKeyBound(dataSlice->UpperLimit().KeyBound, actualUpperBound);
            YT_VERIFY(dataSlice->Tag);
            // Yeah! In the other words, take the value of the tag contained in TLegacyDataSlicePtr pointed by iterator.
            auto tag = *dataSlice->Tag;
            job.AddDataSlice(std::move(*it), tag, /* isPrimary */ true);
        }
        YT_VERIFY(job.GetPrimarySliceCount() > 0);

        job.SetPrimaryLowerBound(actualLowerBound);
        job.SetPrimaryUpperBound(actualUpperBound);

        PrimaryDomains_[EDomainKind::Main].Clear();

        // Perform sanity checks and prepare information for the next sanity check.
        ValidateCurrentJobBounds(actualLowerBound, actualUpperBound);
        PreviousJobUpperBound_ = UpperBound_;
        PreviousJobContainedSingleton_ = CurrentJobContainsSingleton_;
        CurrentJobContainsSingleton_ = false;

        // Now trim foreign data slices. First of all, shorten actual lower and upper bounds
        // in order to respect the foreign comparator length.
        auto shortenedActualLowerBound = ShortenKeyBound(actualLowerBound, ForeignComparator_.GetLength(), RowBuffer_);
        auto shortenedActualUpperBound = ShortenKeyBound(actualUpperBound, ForeignComparator_.GetLength(), RowBuffer_);
        TrimForeignSlices(shortenedActualLowerBound);

        // Finally, iterate over remaining foreign data slices in order to find out which of them should be
        // included to the current job. In general case, this is exactly all foreign data slices, but
        // there are borderline cases with singleton data slices, so we explicitly test each particular data slice.
        // Also, recall that TrimForeignSlices provides us with a guarantee that none of data slices is located
        // to the left of job's range.
        TAggregatedStatistics foreignStatistics;
        for (const auto& dataSlice : ForeignDomain_.DataSlices) {
            if (!ForeignComparator_.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, shortenedActualUpperBound)) {
                YT_VERIFY(dataSlice->Tag);
                job.AddDataSlice(
                    CreateInputDataSlice(dataSlice, ForeignComparator_, shortenedActualLowerBound, shortenedActualUpperBound),
                    *dataSlice->Tag,
                    /* isPrimary */ false);
                foreignStatistics += TAggregatedStatistics::FromDataSlice(dataSlice, /* isPrimary */ false);
            }
        }

        if (!foreignStatistics.IsZero()) {
            YT_LOG_DEBUG_IF(LogDetails_, "Attaching foreign data slices to job (Statistics: %v)", foreignStatistics);
        }

        YT_LOG_DEBUG_IF(
            LogDetails_,
            "Job prepared (DataSlices: %v)",
            job.GetDebugString());

        TotalDataSliceCount_ += job.GetSliceCount();

        ValidateTotalSliceCountLimit();

        return true;
    }

    void ValidateTotalSliceCountLimit() const
    {
        if (TotalDataSliceCount_ > MaxTotalDataSliceCount_) {
            THROW_ERROR_EXCEPTION(EErrorCode::DataSliceLimitExceeded, "Total number of data slices in sorted pool is too large.")
                << TErrorAttribute("total_data_slice_count", TotalDataSliceCount_)
                << TErrorAttribute("max_total_data_slice_count", MaxTotalDataSliceCount_)
                << TErrorAttribute("current_job_count", PreparedJobs_.size());
        }
    }
};

DEFINE_ENUM(ENewEndpointType,
    (Barrier)
    (Foreign)
    (Primary)
);

class TNewSortedJobBuilder
    : public INewSortedJobBuilder
{
public:
    TNewSortedJobBuilder(
        const TSortedJobOptions& options,
        IJobSizeConstraintsPtr jobSizeConstraints,
        const TRowBufferPtr& rowBuffer,
        const std::vector<TInputChunkPtr>& teleportChunks,
        bool inSplit,
        int retryIndex,
        const TInputStreamDirectory& inputStreamDirectory,
        const TLogger& logger)
        : Options_(options)
        , PrimaryComparator_(options.PrimaryComparator)
        , ForeignComparator_(options.ForeignComparator)
        , JobSizeConstraints_(std::move(jobSizeConstraints))
        , JobSampler_(JobSizeConstraints_->GetSamplingRate())
        , RowBuffer_(rowBuffer)
        , InSplit_(inSplit)
        , RetryIndex_(retryIndex)
        , InputStreamDirectory_(inputStreamDirectory)
        , Logger(logger)
    {
        AddTeleportChunkEndpoints(teleportChunks);
    }

    virtual void AddDataSlice(const TLegacyDataSlicePtr& dataSlice) override
    {
        YT_VERIFY(!dataSlice->IsLegacy);
        auto isPrimary = InputStreamDirectory_.GetDescriptor(dataSlice->InputStreamIndex).IsPrimary();

        const auto& comparator = isPrimary ? PrimaryComparator_ : ForeignComparator_;

        if (comparator.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound)) {
            // This can happen if ranges were specified.
            // Chunk slice fetcher can produce empty slices.
            return;
        }

        TEndpoint endpoint = {
            isPrimary ? ENewEndpointType::Primary : ENewEndpointType::Foreign,
            dataSlice,
            dataSlice->LowerLimit().KeyBound,
        };

        Endpoints_.push_back(endpoint);
    }

    virtual std::vector<TNewJobStub> Build() override
    {
        AddPivotKeysEndpoints();
        SortEndpoints();
        if (Options_.LogDetails) {
            LogDetails();
        }
        BuildJobs();

        for (auto& job : Jobs_) {
            job.Finalize(Options_.SortByPosition, PrimaryComparator_);
            ValidateJob(&job);
        }

        return std::move(Jobs_);
    }

    void ValidateJob(const TNewJobStub* job)
    {
        if (job->GetDataWeight() > JobSizeConstraints_->GetMaxDataWeightPerJob()) {
            YT_LOG_DEBUG("Maximum allowed data weight per sorted job exceeds the limit (DataWeight: %v, MaxDataWeightPerJob: %v, "
                "PrimaryLowerBound: %v, PrimaryUpperBound: %v, JobDebugString: %v)",
                job->GetDataWeight(),
                JobSizeConstraints_->GetMaxDataWeightPerJob(),
                job->GetPrimaryLowerBound(),
                job->GetPrimaryUpperBound(),
                job->GetDebugString());

            THROW_ERROR_EXCEPTION(
                EErrorCode::MaxDataWeightPerJobExceeded, "Maximum allowed data weight per sorted job exceeds the limit: %v > %v",
                job->GetDataWeight(),
                JobSizeConstraints_->GetMaxDataWeightPerJob())
                << TErrorAttribute("lower_bound", job->GetPrimaryLowerBound())
                << TErrorAttribute("upper_bound", job->GetPrimaryUpperBound());
        }

        if (job->GetPrimaryDataWeight() > JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob()) {
            YT_LOG_DEBUG("Maximum allowed primary data weight per sorted job exceeds the limit (PrimaryDataWeight: %v, MaxPrimaryDataWeightPerJob: %v, "
                "PrimaryLowerBound: %v, PrimaryUpperBound: %v, JobDebugString: %v)",
                job->GetPrimaryDataWeight(),
                JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob(),
                job->GetPrimaryLowerBound(),
                job->GetPrimaryUpperBound(),
                job->GetDebugString());

            THROW_ERROR_EXCEPTION(
                EErrorCode::MaxPrimaryDataWeightPerJobExceeded, "Maximum allowed primary data weight per sorted job exceeds the limit: %v > %v",
                job->GetPrimaryDataWeight(),
                JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob())
                << TErrorAttribute("lower_bound", job->GetPrimaryLowerBound())
                << TErrorAttribute("upper_bound", job->GetPrimaryUpperBound());
        }
    }

    virtual i64 GetTotalDataSliceCount() const override
    {
        return TotalDataSliceCount_;
    }

private:
    TSortedJobOptions Options_;

    TComparator PrimaryComparator_;
    TComparator ForeignComparator_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSampler JobSampler_;

    TRowBufferPtr RowBuffer_;

    struct TEndpoint
    {
        ENewEndpointType Type;
        TLegacyDataSlicePtr DataSlice;
        TKeyBound KeyBound;
    };

    //! Endpoints of primary table slices in SortedReduce and SortedMerge.
    std::vector<TEndpoint> Endpoints_;

    //! Vector keeping the pool-side state of all jobs that depend on the data from this pool.
    //! These items are merely stubs of a future jobs that are filled during the BuildJobsBy{Key/TableIndices}()
    //! call, and when current job is finished it is passed to the `JobManager_` that becomes responsible
    //! for its future.
    std::vector<TNewJobStub> Jobs_;

    int JobIndex_ = 0;
    i64 TotalDataWeight_ = 0;

    i64 TotalDataSliceCount_ = 0;

    //! Indicates if this sorted job builder is used during job splitting.
    bool InSplit_ = false;

    int RetryIndex_;

    const TInputStreamDirectory& InputStreamDirectory_;

    const TLogger& Logger;

    void AddPivotKeysEndpoints()
    {
        for (const auto& pivotKey : Options_.PivotKeys) {
            // Pivot keys act as key bounds of type >=.
            TEndpoint endpoint = {
                ENewEndpointType::Barrier,
                nullptr,
                TKeyBound::FromRow(pivotKey, /* isInclusive */ true, /* isUpper */ false),
            };
            Endpoints_.emplace_back(endpoint);
        }
    }

    void AddTeleportChunkEndpoints(const std::vector<TInputChunkPtr>& teleportChunks)
    {
        for (const auto& inputChunk : teleportChunks) {
            auto minKeyRow = RowBuffer_->Capture(inputChunk->BoundaryKeys()->MinKey.Begin(), PrimaryComparator_.GetLength());
            Endpoints_.emplace_back(TEndpoint{
                .Type = ENewEndpointType::Barrier,
                .DataSlice = nullptr,
                // NB: we put barrier of type >minKey intentionally. Otherwise in case when EnableKeyGuarantee = false
                // and there is a singleton data slice consisting exactly of minKey, we may join it together with
                // some data slice to the right of teleport chunk leading to the sort order violation (resulting job
                // will overlap with the teleport chunk).
                .KeyBound = TKeyBound::FromRow(minKeyRow, /* isInclusive */ false, /* isUpper */ false),
            });
        }
    }

    void SortEndpoints()
    {
        YT_LOG_DEBUG("Sorting endpoints (Count: %v)", Endpoints_.size());
        // We sort endpoints by their location. In each group of endpoints at the same point
        // we sort them by type: barriers first, then foreign endpoints, then primary ones.
        std::sort(
            Endpoints_.begin(),
            Endpoints_.end(),
            [=] (const TEndpoint& lhs, const TEndpoint& rhs) {
                auto result = PrimaryComparator_.CompareKeyBounds(lhs.KeyBound, rhs.KeyBound);
                if (result != 0) {
                    return result < 0;
                }
                return static_cast<int>(lhs.Type) < static_cast<int>(rhs.Type);
            });
    }

    void LogDetails()
    {
        for (int index = 0; index < Endpoints_.size(); ++index) {
            const auto& endpoint = Endpoints_[index];
            YT_LOG_DEBUG("Endpoint (Index: %v, KeyBound: %v, Type: %v, DataSlice: %v)",
                index,
                endpoint.KeyBound,
                endpoint.Type,
                endpoint.DataSlice.Get());
        }
    }

    i64 GetDataWeightPerJob() const
    {
        return
            JobSizeConstraints_->GetSamplingRate()
            ? JobSizeConstraints_->GetSamplingDataWeightPerJob()
            : JobSizeConstraints_->GetDataWeightPerJob();
    }

    i64 GetPrimaryDataWeightPerJob() const
    {
        return
            JobSizeConstraints_->GetSamplingRate()
            ? JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob()
            : JobSizeConstraints_->GetPrimaryDataWeightPerJob();
    }

    void AddJob(TNewJobStub& job)
    {
        if (JobSampler_.Sample()) {
            YT_LOG_DEBUG("Sorted job created (JobIndex: %v, BuiltJobCount: %v, PrimaryDataSize: %v, PrimaryRowCount: %v, "
                "PrimarySliceCount: %v, PreliminaryForeignDataSize: %v, PreliminaryForeignRowCount: %v, "
                "PreliminaryForeignSliceCount: %v, PrimaryLowerBound: %v, PrimaryUpperBound: %v)",
                JobIndex_,
                Jobs_.size(),
                job.GetPrimaryDataWeight(),
                job.GetPrimaryRowCount(),
                job.GetPrimarySliceCount(),
                job.GetPreliminaryForeignDataWeight(),
                job.GetPreliminaryForeignRowCount(),
                job.GetPreliminaryForeignSliceCount(),
                job.GetPrimaryLowerBound(),
                job.GetPrimaryUpperBound());

            TotalDataWeight_ += job.GetDataWeight();

            if (Options_.LogDetails) {
                YT_LOG_DEBUG("Sorted job details (JobIndex: %v, BuiltJobCount: %v, Details: %v)",
                    JobIndex_,
                    Jobs_.size(),
                    job.GetDebugString());
            }

            Jobs_.emplace_back(std::move(job));
        } else {
            YT_LOG_DEBUG("Sorted job skipped (JobIndex: %v, BuiltJobCount: %v, PrimaryDataSize: %v, "
                "PreliminaryForeignDataSize: %v, PrimaryLowerBound: %v, PrimaryUpperBound: %v)",
                JobIndex_,
                static_cast<int>(Jobs_.size()),
                job.GetPrimaryDataWeight(),
                job.GetPreliminaryForeignDataWeight(),
                job.GetPrimaryLowerBound(),
                job.GetPrimaryUpperBound());
        }
        ++JobIndex_;
    }

    void BuildJobs()
    {
        if (auto samplingRate = JobSizeConstraints_->GetSamplingRate()) {
            YT_LOG_DEBUG(
                "Building jobs with sampling "
                "(SamplingRate: %v, SamplingDataWeightPerJob: %v, SamplingPrimaryDataWeightPerJob: %v)",
                *JobSizeConstraints_->GetSamplingRate(),
                JobSizeConstraints_->GetSamplingDataWeightPerJob(),
                JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob());
        }

        auto yielder = CreatePeriodicYielder();

        double retryFactor = std::pow(JobSizeConstraints_->GetDataWeightPerJobRetryFactor(), RetryIndex_);

        TStagingArea stagingArea(
            Options_.EnableKeyGuarantee,
            PrimaryComparator_,
            ForeignComparator_,
            RowBuffer_,
            TAggregatedStatistics{
                .DataSliceCount = JobSizeConstraints_->GetMaxDataSlicesPerJob(),
                .DataWeight = static_cast<i64>(std::min<double>(
                    std::numeric_limits<i64>::max() / 2,
                    GetDataWeightPerJob() * retryFactor)),
                .PrimaryDataWeight = static_cast<i64>(std::min<double>(
                    std::numeric_limits<i64>::max() / 2,
                    GetPrimaryDataWeightPerJob() * retryFactor))
            },
            Options_.MaxTotalSliceCount,
            JobSizeConstraints_->GetInputSliceDataWeight(),
            InputStreamDirectory_,
            Options_.LogDetails,
            Logger);

        // Iterate over groups of coinciding endpoints.
        for (int startIndex = 0, endIndex = 0; startIndex < Endpoints_.size(); startIndex = endIndex) {
            yielder.TryYield();

            // Extract contiguous group of endpoints.
            while (
                endIndex != Endpoints_.size() &&
                PrimaryComparator_.CompareKeyBounds(Endpoints_[startIndex].KeyBound, Endpoints_[endIndex].KeyBound) == 0)
            {
                ++endIndex;
            }

            stagingArea.PromoteUpperBound(Endpoints_[startIndex].KeyBound.Invert());

            // No need to add more than one barrier at the same point, so keep track if this has already happened.
            bool barrierAdded = false;

            for (const auto& endpoint : MakeRange(Endpoints_).Slice(startIndex, endIndex)) {
                switch (endpoint.Type) {
                    case ENewEndpointType::Barrier:
                        if (!barrierAdded) {
                            stagingArea.Flush(/* force */ true);
                            stagingArea.PutBarrier();
                            barrierAdded = true;
                        }
                        break;
                    case ENewEndpointType::Foreign:
                    case ENewEndpointType::Primary:
                        stagingArea.Put(
                            endpoint.DataSlice,
                            InputStreamDirectory_.GetDescriptor(endpoint.DataSlice->InputStreamIndex).IsPrimary());
                        break;
                    default:
                        YT_ABORT();
                }
            }

            // Pivot keys provide guarantee that we won't introduce more jobs than
            // defined by them, so we do not try to flush by ourself if they are present.
            if (Options_.PivotKeys.empty()) {
                stagingArea.Flush(/* force */ false);
            }
        }

        stagingArea.Finish();

        for (auto& preparedJob : stagingArea.PreparedJobs()) {
            yielder.TryYield();

            if (preparedJob.GetIsBarrier()) {
                Jobs_.emplace_back(std::move(preparedJob));
            } else {
                AddJob(preparedJob);
            }
        }

        JobSizeConstraints_->UpdateInputDataWeight(TotalDataWeight_);

        YT_LOG_DEBUG("Jobs created (Count: %v)", Jobs_.size());

        if (InSplit_ && Jobs_.size() == 1 && JobSizeConstraints_->GetJobCount() > 1) {
            YT_LOG_DEBUG("Pool was not able to split job properly (SplitJobCount: %v, JobCount: %v)",
                JobSizeConstraints_->GetJobCount(),
                Jobs_.size());

            Jobs_.front().SetUnsplittable();
        }

        TotalDataSliceCount_ = stagingArea.GetTotalDataSliceCount();
    }

    TPeriodicYielder CreatePeriodicYielder()
    {
        if (Options_.EnablePeriodicYielder) {
            return TPeriodicYielder(PrepareYieldPeriod);
        } else {
            return TPeriodicYielder();
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TNewSortedJobBuilder)

////////////////////////////////////////////////////////////////////////////////

INewSortedJobBuilderPtr CreateNewSortedJobBuilder(
    const TSortedJobOptions& options,
    IJobSizeConstraintsPtr jobSizeConstraints,
    const TRowBufferPtr& rowBuffer,
    const std::vector<TInputChunkPtr>& teleportChunks,
    bool inSplit,
    int retryIndex,
    const TInputStreamDirectory& inputStreamDirectory,
    const TLogger& logger)
{
    return New<TNewSortedJobBuilder>(
        options,
        std::move(jobSizeConstraints),
        rowBuffer,
        teleportChunks,
        inSplit,
        retryIndex,
        inputStreamDirectory,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
