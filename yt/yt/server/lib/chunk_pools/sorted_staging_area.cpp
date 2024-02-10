#include "sorted_staging_area.h"

#include "input_stream.h"
#include "job_size_tracker.h"
#include "new_job_manager.h"

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/core/misc/heap.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

// == IMPLEMENTATION DETAILS ==
//
// All data slices in the staging area are divided into four domains.
//
// === Main ===
//
// Contains data slices such that are going in the next job to be built. On flush they are
// cut using current upper bound into two halves, first of which goes to the job, while
// second goes to the Buffer.
//
// Invariant: for all main data slices D condition D.lowerBound < UpperBound_ holds.
//
// === Buffer ===
//
// Contains data slices that appeared at the same time upper bound took its current place.
//
// Invariants:
// 1) for all buffer data slices D holds D.LowerBound == UpperBound_.Invert().
// 2) if key guarantee is disabled, contains only non-solid (i.e. non-row-sliced) data slices.
//
// === Solid ===
//
// Similar to the previous one, but appears only when key guarantee is disabled and contains
// only row-sliced data slices. These data slices must not be additionally key sliced, so
// we distinguish them from buffer data slices.
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
//      <Main>               )               <Buffer>
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
// Slices B, D and E are in Main domain, slices A and C are in Buffer domain.
// Slice C is a single-key slice, but it was also put as a regular Buffer slice.
// by a caller.
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
// A:                        )[-------]      <-- <Buffer>
// B:              [---------)
// C1:                       )[]             <\
// C2:                       )[]             < - <Solid>
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
// Note that solid slices will also be included to current job even though they are not
// inside upper bound.
//
//
// 3) EnableKeyGuarantee = true, foreign data is present.
//                       inclusive
//                      upper bound
//      <Main>               ]               <Buffer>
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

//! This class is responsible for holding the current "working set" of data slices.
class TSortedStagingArea
    : public ISortedStagingArea
{
public:
    TSortedStagingArea(
        bool enableKeyGuarantee,
        TComparator primaryComparator,
        TComparator foreignComparator,
        const TRowBufferPtr& rowBuffer,
        i64 initialTotalDataSliceCount,
        i64 maxTotalDataSliceCount,
        const TInputStreamDirectory& inputStreamDirectory,
        const TLogger& logger)
        : EnableKeyGuarantee_(enableKeyGuarantee)
        , PrimaryComparator_(primaryComparator)
        , ForeignComparator_(foreignComparator)
        , MaxTotalDataSliceCount_(maxTotalDataSliceCount)
        , RowBuffer_(rowBuffer)
        , InputStreamDirectory_(inputStreamDirectory)
        , Logger(logger)
        , TotalDataSliceCount_(initialTotalDataSliceCount)
        , MainDomain_("Main", logger, PrimaryComparator_)
        , BufferDomain_("Buffer", logger, PrimaryComparator_)
        , ForeignDomain_(ForeignComparator_)
    {
        YT_LOG_DEBUG("Staging area instantiated");

        if (!EnableKeyGuarantee_) {
            SolidDomain_ = TPrimaryDomain("Solid", logger, PrimaryComparator_);
        }
    }

    void PromoteUpperBound(TKeyBound newUpperBound) override
    {
        YT_LOG_TRACE("Promoting upper bound (NewUpperBound: %v)", newUpperBound);

        YT_VERIFY(PrimaryComparator_.CompareKeyBounds(UpperBound_, newUpperBound) <= 0);

        // Buffer slices are not attached to current upper bound any more, so they
        // should me moved to the main area.
        TransferBufferToMain();
        TransferSolidToMain();

        UpperBound_ = newUpperBound;
    }

    void Put(TLegacyDataSlicePtr dataSlice, ESliceType sliceType) override
    {
        YT_VERIFY(dataSlice->Tag);

        switch (sliceType) {
            case ESliceType::Buffer:
                YT_VERIFY(dataSlice->LowerLimit().KeyBound == UpperBound_.Invert());
                BufferDomain_.PushBack(std::move(dataSlice));
                break;
            case ESliceType::Solid:
                YT_VERIFY(
                    dataSlice->LowerLimit().KeyBound == UpperBound_.Invert() ||
                    (dataSlice->LowerLimit().KeyBound.IsInclusive &&
                    dataSlice->LowerLimit().KeyBound == UpperBound_.Invert().ToggleInclusiveness()));
                SolidDomain_.PushBack(std::move(dataSlice));
                break;
            case ESliceType::Foreign:
                ForeignDomain_.AddDataSlice(std::move(dataSlice));
                break;
            default:
                YT_ABORT();
        }
    }

    void PutBarrier() override
    {
        auto& job = PreparedJobs_.emplace_back();
        job.SetIsBarrier(true);
    }

    void Flush() override
    {
        // If we have no Main nor Solid slices, we have nothing to do.
        if (IsExhausted()) {
            YT_LOG_TRACE("Nothing to flush in staging area");
            // Nothing to flush.
            return;
        }

        YT_LOG_TRACE(
            "Performing flush in staging area (Statistics: %v)",
            GetStatisticsDebugString());

        // By this moment singleton slices are not yet in the Main, so we cut only
        // proper Main data slices.
        CutNonSolidMainByUpperBound();

        // By this moment some part of singleton jobs could have been added
        // to main. Now flush Main domain into job.
        DoFlush();

        YT_LOG_TRACE(
            "Staging area flushed (Statistics: %v)",
            GetStatisticsDebugString());

        // Make a sanity check that Main domain is empty.
        YT_VERIFY(MainDomain_.DataSlices.empty());
        YT_VERIFY(MainDomain_.Statistics.IsZero());
    }

    //! Called at the end of processing to flush all remaining data slices into jobs.
    void Finish() override
    {
        YT_LOG_TRACE("Finishing work in staging area");

        PromoteUpperBound(TKeyBound::MakeUniversal(/*isUpper*/ true));

        Flush();
        for (const auto& domain : {MainDomain_, BufferDomain_}) {
            YT_VERIFY(domain.DataSlices.empty());
            YT_VERIFY(domain.Statistics.IsZero());
        }
    }

    std::vector<TNewJobStub>& PreparedJobs() override
    {
        return PreparedJobs_;
    }

    //! Total number of data slices in all created jobs.
    //! Used for internal bookkeeping by the outer code.
    i64 GetTotalDataSliceCount() const override
    {
        return TotalDataSliceCount_;
    }

    TKeyBound GetPrimaryUpperBound() const override
    {
        TKeyBound upperBound = UpperBound_;
        for (const auto* domain : {&MainDomain_, &BufferDomain_, &SolidDomain_}) {
            if (auto domainUpperBound = domain->GetUpperBound()) {
                upperBound = PrimaryComparator_.WeakerKeyBound(upperBound, domainUpperBound);
            }
        }
        return upperBound;
    }

    TResourceVector GetForeignResourceVector() const override
    {
        return ForeignDomain_.Statistics;
    }

private:
    bool EnableKeyGuarantee_;
    TComparator PrimaryComparator_;
    TComparator ForeignComparator_;
    i64 MaxTotalDataSliceCount_;
    TRowBufferPtr RowBuffer_;
    TInputStreamDirectory InputStreamDirectory_;
    TLogger Logger;

    //! Upper bound using which all data slices in Main domain are to be cut.
    //! NB: actual upper bound of job to be built may differ from #UpperBound_
    //! in case when singleton data slices are added to the job; in this case
    //! actual upper bound for a job will be #UpperBound_.ToggleInclusiveness()
    //! (i.e. exclusive instead of inclusive).
    TKeyBound UpperBound_ = TKeyBound::MakeEmpty(/*isUpper*/ true);

    i64 TotalDataSliceCount_;
    std::vector<TNewJobStub> PreparedJobs_;

    //! These flags are used only for internal sanity check.
    bool PreviousJobContainedSolidSlices_ = false;

    //! Previous job upper bound, used for internal sanity check.
    TKeyBound PreviousJobUpperBound_ = TKeyBound::MakeEmpty(/*isUpper*/ true);

    //! Structure holding data slices for one of primary domains with their aggregated statistics.
    class TPrimaryDomain
    {
    public:
        TResourceVector Statistics;
        std::deque<TLegacyDataSlicePtr> DataSlices;
        TLogger Logger;

        DEFINE_BYVAL_RO_PROPERTY(TKeyBound, UpperBound);

    public:
        TPrimaryDomain()
            : Enabled_(false)
        { }

        TPrimaryDomain(TStringBuf kind, const TLogger& logger, TComparator comparator)
            : Logger(logger.WithTag("Domain: %v", kind))
            , Comparator_(std::move(comparator))
            , Enabled_(true)
        { }

        bool IsEmpty() const
        {
            return Statistics.IsZero();
        }

        void PushBack(TLegacyDataSlicePtr dataSlice)
        {
            YT_VERIFY(Enabled_);

            UpdateUpperBound(dataSlice->UpperLimit().KeyBound);

            YT_LOG_TRACE("Pushing to domain back (DataSlice: %v)", GetDataSliceDebugString(dataSlice));
            Statistics += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ true);
            DataSlices.push_back(std::move(dataSlice));
        }

        void PushFront(TLegacyDataSlicePtr dataSlice)
        {
            YT_VERIFY(Enabled_);

            UpdateUpperBound(dataSlice->UpperLimit().KeyBound);

            YT_LOG_TRACE("Pushing to domain front (DataSlice: %v)", GetDataSliceDebugString(dataSlice));
            Statistics += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ true);
            DataSlices.push_front(std::move(dataSlice));
        }

        void Clear()
        {
            Statistics = TResourceVector();
            DataSlices.clear();
            UpperBound_ = TKeyBound();
        }

    private:
        TComparator Comparator_;
        bool Enabled_;

        void UpdateUpperBound(TKeyBound upperBound)
        {
            if (!UpperBound_ || Comparator_.CompareKeyBounds(UpperBound_, upperBound) < 0) {
                UpperBound_ = upperBound;
            }
        }
    };

    //! Similar to previous, but for foreign data slices.
    struct TForeignDomain
    {
        TResourceVector Statistics;

        //! Per-stream queue of data slices.
        std::vector<std::deque<TLegacyDataSlicePtr>> StreamIndexToDataSlices;

        using TStreamIndex = int;

        //! Heap of stream indices ordered by front data slice upper bounds.
        //! Empty streams are not present in the heap.
        std::vector<TStreamIndex> StreamHeap;
        std::function<bool(int, int)> Comparator;

        explicit TForeignDomain(const TComparator& foreignComparator)
            : Comparator([&foreignComparator, this] (int lhsIndex, int rhsIndex) {
                YT_VERIFY(lhsIndex < std::ssize(StreamIndexToDataSlices));
                YT_VERIFY(!StreamIndexToDataSlices[lhsIndex].empty());
                const auto& lhsDataSlice = StreamIndexToDataSlices[lhsIndex].front();
                YT_VERIFY(rhsIndex < std::ssize(StreamIndexToDataSlices));
                YT_VERIFY(!StreamIndexToDataSlices[rhsIndex].empty());
                const auto& rhsDataSlice = StreamIndexToDataSlices[rhsIndex].front();

                return foreignComparator.CompareKeyBounds(lhsDataSlice->UpperLimit().KeyBound, rhsDataSlice->UpperLimit().KeyBound) < 0;
            })
        { }

        void AddDataSlice(TLegacyDataSlicePtr dataSlice)
        {
            auto streamIndex = dataSlice->GetInputStreamIndex();
            if (streamIndex >= std::ssize(StreamIndexToDataSlices)) {
                StreamIndexToDataSlices.resize(streamIndex + 1);
            }

            bool wasEmpty = StreamIndexToDataSlices[streamIndex].empty();
            Statistics += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ false);

            StreamIndexToDataSlices[streamIndex].push_back(std::move(dataSlice));

            if (wasEmpty) {
                StreamHeap.push_back(streamIndex);
                AdjustHeapBack(StreamHeap.begin(), StreamHeap.end(), Comparator);
            }
        }

        //! Returns smallest data slice according to comparator or nullptr if empty.
        TLegacyDataSlicePtr Front() const
        {
            return StreamHeap.empty() ? nullptr : StreamIndexToDataSlices[StreamHeap.front()].front();
        }

        void Pop()
        {
            YT_VERIFY(!StreamHeap.empty());
            auto streamIndex = StreamHeap.front();
            auto& dataSlices = StreamIndexToDataSlices[streamIndex];
            Statistics -= TResourceVector::FromDataSlice(dataSlices.front(), /*isPrimary*/ false);
            ExtractHeap(StreamHeap.begin(), StreamHeap.end(), Comparator);
            StreamHeap.pop_back();
            dataSlices.pop_front();
            if (!dataSlices.empty()) {
                StreamHeap.push_back(streamIndex);
                AdjustHeapBack(StreamHeap.begin(), StreamHeap.end(), Comparator);
            }
        }
    };

    TPrimaryDomain MainDomain_;
    TPrimaryDomain SolidDomain_;
    TPrimaryDomain BufferDomain_;
    TForeignDomain ForeignDomain_;
    std::vector<TLegacyDataSlicePtr> NonSolidMainDataSlices_;

    TResourceVector GetTotalStatistics() const
    {
        return
            MainDomain_.Statistics +
            SolidDomain_.Statistics +
            BufferDomain_.Statistics +
            ForeignDomain_.Statistics;
    }

    TString GetStatisticsDebugString() const
    {
        std::vector<TString> parts;
        parts.emplace_back(Format("Main: %v", MainDomain_.Statistics));
        parts.emplace_back(Format("Buffer: %v", BufferDomain_.Statistics));
        return Format("{%v}", JoinToString(parts, TStringBuf(", ")));
    }

    //! Check if we have at least one data slice to build job right now.
    bool IsExhausted() const
    {
        return MainDomain_.IsEmpty() && SolidDomain_.IsEmpty();
    }

    void CutNonSolidMainByUpperBound()
    {
        YT_LOG_TRACE("Cutting non-solid main domain by upper bound (UpperBound: %v)", UpperBound_);

        // We first collect data slice to push to buffer domain, and only then push them.
        // Since we are pushing to domain front (see comments below), we must push all
        // data slices we want in reverse order in order to keep relative order of slices.
        std::vector<TLegacyDataSlicePtr> toBuffer;

        for (auto& dataSlice : NonSolidMainDataSlices_) {
            YT_VERIFY(!PrimaryComparator_.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound));
            YT_VERIFY(PrimaryComparator_.CompareKeyBounds(dataSlice->LowerLimit().KeyBound, UpperBound_) <= 0);

            TLegacyDataSlicePtr restDataSlice;

            if (!PrimaryComparator_.IsRangeEmpty(UpperBound_.Invert(), dataSlice->UpperLimit().KeyBound)) {
                // Right part of the data slice goes to Buffer domain.
                auto restDataSlice = CreateInputDataSlice(dataSlice, PrimaryComparator_, UpperBound_.Invert(), dataSlice->UpperLimit().KeyBound);
                restDataSlice->LowerLimit().KeyBound = PrimaryComparator_.StrongerKeyBound(UpperBound_.Invert(), restDataSlice->LowerLimit().KeyBound);
                // It may happen that data slice is entirely inside current upper bound (e.g. slice E from example 1 above), so
                // check if rest data slice is non-empty.
                if (!PrimaryComparator_.IsRangeEmpty(restDataSlice->LowerLimit().KeyBound, restDataSlice->UpperLimit().KeyBound)) {
                    restDataSlice->CopyPayloadFrom(*dataSlice);
                    // Refer to explanation in YT-14566 for more details.
                    // PushFront is crucial!
                    toBuffer.emplace_back(std::move(restDataSlice));
                }
                // Left part of the data slice resides in the Main domain.
                dataSlice->UpperLimit().KeyBound = UpperBound_;
            }

            YT_VERIFY(PrimaryComparator_.CompareKeyBounds(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound) <= 0);
        }

        NonSolidMainDataSlices_.clear();

        for (auto& dataSlice : Reversed(toBuffer)) {
            auto& destinationDomain = PrimaryComparator_.IsInteriorEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound) && !EnableKeyGuarantee_
                ? SolidDomain_
                : BufferDomain_;
            destinationDomain.PushFront(std::move(dataSlice));
        }
    }

    void TransferBufferToMain()
    {
        // NB: it is important to transfer singletons before non-singletons;
        // otherwise we would violate slice order guarantee.
        for (auto& dataSlice : BufferDomain_.DataSlices) {
            YT_VERIFY(dataSlice->LowerLimit().KeyBound == UpperBound_.Invert());
            NonSolidMainDataSlices_.push_back(dataSlice);
            MainDomain_.PushBack(std::move(dataSlice));
        }
        BufferDomain_.Clear();
    }

    void TransferSolidToMain()
    {
        for (const auto& dataSlice : SolidDomain_.DataSlices) {
            YT_VERIFY(
                dataSlice->LowerLimit().KeyBound == UpperBound_.Invert() ||
                (dataSlice->LowerLimit().KeyBound.IsInclusive &&
                dataSlice->LowerLimit().KeyBound == UpperBound_.Invert().ToggleInclusiveness()));
            MainDomain_.PushBack(std::move(dataSlice));
        }
        SolidDomain_.Clear();
    }

    void ValidateCurrentJobBounds(TKeyBound actualLowerBound, TKeyBound actualUpperBound) const
    {
        YT_LOG_TRACE(
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
        // But if the previous job contained solid slice, it spanned a bit wider,
        // including one extra key (obtained from singleton slice). In this case
        // picture may look like the following:
        //
        // C: --------------[---------------]---
        // P: ----[-------------]---------------
        // solid slice:     [---]

        // We assert that the previous job is located to the left from the
        // current one unless previous job contained solid slices.

        if (!PreviousJobContainedSolidSlices_) {
            YT_VERIFY(PrimaryComparator_.CompareKeyBounds(actualLowerBound, PreviousJobUpperBound_) >= 0);
        }
    }

    //! Trim leftmost foreign slices (in respect to their upper limits) until
    //! leftmost of them starts to intersect the lower bound of current job.
    void TrimForeignSlices(TKeyBound actualLowerBound)
    {
        while (true) {
            auto smallestForeignDataSlice = ForeignDomain_.Front();
            // Check if foreign domain is exhausted.
            if (!smallestForeignDataSlice) {
                break;
            }
            // Check if smallest slice should be trimmed or not.
            if (!ForeignComparator_.IsRangeEmpty(actualLowerBound, smallestForeignDataSlice->UpperLimit().KeyBound)) {
                break;
            }
            YT_LOG_TRACE("Trimming foreign data slice (DataSlice: %v)", smallestForeignDataSlice);
            ForeignDomain_.Pop();
        }
    }

    //! If there is at least one data slice in the main domain, form a job and return true.
    //! Otherwise, return false.
    void DoFlush()
    {
        YT_VERIFY(!IsExhausted());

        auto& job = PreparedJobs_.emplace_back();

        YT_LOG_TRACE(
            "Flushing main and solid domains into job (MainStatistics: %v, SolidStatistics: %v)",
            MainDomain_.Statistics,
            SolidDomain_.Statistics);

        // Calculate the actual lower and upper bounds of newly formed job and move data slices to the job.
        auto actualLowerBound = TKeyBound::MakeEmpty(/*isUpper*/ false);
        auto actualUpperBound = TKeyBound::MakeEmpty(/*isUpper*/ true);

        for (auto* domain : {&MainDomain_, &SolidDomain_}) {
            for (auto& dataSlice : domain->DataSlices) {
                actualLowerBound = PrimaryComparator_.WeakerKeyBound(dataSlice->LowerLimit().KeyBound, actualLowerBound);
                actualUpperBound = PrimaryComparator_.WeakerKeyBound(dataSlice->UpperLimit().KeyBound, actualUpperBound);
                YT_VERIFY(dataSlice->Tag);
                auto tag = *dataSlice->Tag;
                job.AddDataSlice(std::move(dataSlice), tag, /*isPrimary*/ true);
                const auto& Logger = domain->Logger;
                YT_LOG_TRACE(
                    "Adding primary data slice to job (DataSlice: %v)",
                    GetDataSliceDebugString(dataSlice));
            }
        }
        YT_VERIFY(job.GetPrimarySliceCount() > 0);

        job.SetPrimaryLowerBound(actualLowerBound);
        job.SetPrimaryUpperBound(actualUpperBound);

        MainDomain_.Clear();
        SolidDomain_.Clear();

        // Perform sanity checks and prepare information for the next sanity check.
        ValidateCurrentJobBounds(actualLowerBound, actualUpperBound);
        PreviousJobUpperBound_ = UpperBound_;
        PreviousJobContainedSolidSlices_ = SolidDomain_.Statistics.IsZero();

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
        TResourceVector foreignStatistics;
        for (const auto& dataSlices : ForeignDomain_.StreamIndexToDataSlices) {
            for (const auto& dataSlice : dataSlices) {
                if (!ForeignComparator_.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, shortenedActualUpperBound)) {
                    YT_VERIFY(dataSlice->Tag);
                    job.AddDataSlice(
                        CreateInputDataSlice(dataSlice, ForeignComparator_, shortenedActualLowerBound, shortenedActualUpperBound),
                        *dataSlice->Tag,
                        /*isPrimary*/ false);
                    YT_LOG_TRACE(
                        "Adding foreign data slice to job (DataSlice: %v)",
                        GetDataSliceDebugString(dataSlice));
                    foreignStatistics += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ false);
                }
            }
        }

        if (!foreignStatistics.IsZero()) {
            YT_LOG_TRACE("Attaching foreign data slices to job (Statistics: %v)", foreignStatistics);
        }

        YT_LOG_TRACE("Job prepared (DataSlices: %v)", job.GetDebugString());

        TotalDataSliceCount_ += job.GetSliceCount();

        ValidateTotalSliceCountLimit();
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

////////////////////////////////////////////////////////////////////////////////

ISortedStagingAreaPtr CreateSortedStagingArea(
    bool enableKeyGuarantee,
    TComparator primaryComparator,
    TComparator foreignComparator,
    const TRowBufferPtr& rowBuffer,
    i64 initialTotalDataSliceCount,
    i64 maxTotalDataSliceCount,
    const TInputStreamDirectory& inputStreamDirectory,
    const TLogger& logger)
{
    return New<TSortedStagingArea>(
        enableKeyGuarantee,
        primaryComparator,
        foreignComparator,
        rowBuffer,
        initialTotalDataSliceCount,
        maxTotalDataSliceCount,
        inputStreamDirectory,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
