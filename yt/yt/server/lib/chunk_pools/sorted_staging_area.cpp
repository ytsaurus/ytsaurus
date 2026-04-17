#include "sorted_staging_area.h"

#include "new_job_manager.h"

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/core/misc/heap.h>

#include <util/generic/adaptor.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NLogging;
using namespace NTableClient;

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

namespace {

////////////////////////////////////////////////////////////////////////////////

//! Structure holding data slices for one of primary domains with their aggregated statistics.
class TPrimaryDomain
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TKeyBound, UpperBound);
    DEFINE_BYVAL_RO_PROPERTY(TResourceVector, Statistics);

public:
    TPrimaryDomain(TStringBuf kind, TLogger logger, const TComparator& comparator)
        : Logger(std::move(logger).WithTag("Domain: %v", kind))
        , Comparator_(comparator)
    { }

    bool IsEmpty() const
    {
        YT_VERIFY(Statistics_.IsZero() == DataSlices_.empty());
        return Statistics_.IsZero();
    }

    void PushBack(TLegacyDataSlicePtr dataSlice)
    {
        UpdateUpperBound(dataSlice->UpperLimit().KeyBound);

        YT_LOG_TRACE("Pushing to domain back (DataSlice: %v)", GetDataSliceDebugString(dataSlice));
        Statistics_ += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ true);
        DataSlices_.push_back(std::move(dataSlice));
    }

    void PushFront(TLegacyDataSlicePtr dataSlice)
    {
        UpdateUpperBound(dataSlice->UpperLimit().KeyBound);

        YT_LOG_TRACE("Pushing to domain front (DataSlice: %v)", GetDataSliceDebugString(dataSlice));
        Statistics_ += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ true);
        DataSlices_.push_front(std::move(dataSlice));
    }

    std::deque<TLegacyDataSlicePtr> ExtractDataSlicesAndClear()
    {
        Statistics_ = TResourceVector();
        UpperBound_ = TKeyBound();
        return std::exchange(DataSlices_, {});
    }

    const TLogger& GetLogger() const
    {
        return Logger;
    }

private:
    TLogger Logger;
    const TComparator& Comparator_;
    std::deque<TLegacyDataSlicePtr> DataSlices_;

    void UpdateUpperBound(TKeyBound upperBound)
    {
        if (!UpperBound_ || Comparator_.CompareKeyBounds(UpperBound_, upperBound) < 0) {
            UpperBound_ = upperBound;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Similar to previous, but for foreign data slices.
class TForeignDomain
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TResourceVector, Statistics);

    //! Per-stream queue of data slices.
    DEFINE_BYREF_RO_PROPERTY(std::vector<std::deque<TLegacyDataSlicePtr>>, StreamIndexToDataSlices);

public:
    explicit TForeignDomain(const TComparator& foreignComparator)
        : ForeignComparator_(foreignComparator)
    { }

    //! Returns smallest data slice according to comparator or nullptr if empty.
    TLegacyDataSlicePtr Front() const
    {
        return StreamHeap_.empty() ? nullptr : StreamIndexToDataSlices_[StreamHeap_.front()].front();
    }

    void AddDataSlice(TLegacyDataSlicePtr dataSlice)
    {
        int streamIndex = dataSlice->GetInputStreamIndex();
        if (streamIndex >= std::ssize(StreamIndexToDataSlices_)) {
            StreamIndexToDataSlices_.resize(streamIndex + 1);
        }

        bool wasEmpty = StreamIndexToDataSlices_[streamIndex].empty();
        Statistics_ += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ false);

        StreamIndexToDataSlices_[streamIndex].push_back(std::move(dataSlice));

        if (wasEmpty) {
            AddToHeap(streamIndex);
        }
    }

    void Pop()
    {
        YT_VERIFY(!StreamHeap_.empty());
        int streamIndex = StreamHeap_.front();
        auto& dataSlices = StreamIndexToDataSlices_[streamIndex];
        Statistics_ -= TResourceVector::FromDataSlice(dataSlices.front(), /*isPrimary*/ false);
        PopFromHeap();
        dataSlices.pop_front();
        if (!dataSlices.empty()) {
            AddToHeap(streamIndex);
        }
    }

private:
    //! Heap of stream indices ordered by front data slice upper bounds.
    //! Empty streams are not present in the heap.
    std::vector<int> StreamHeap_;
    const TComparator& ForeignComparator_;

    auto GetStreamComparator() const
    {
        return [this] (int lhsIndex, int rhsIndex) {
            YT_VERIFY(lhsIndex < std::ssize(StreamIndexToDataSlices_));
            YT_VERIFY(!StreamIndexToDataSlices_[lhsIndex].empty());
            const auto& lhsDataSlice = StreamIndexToDataSlices_[lhsIndex].front();
            YT_VERIFY(rhsIndex < std::ssize(StreamIndexToDataSlices_));
            YT_VERIFY(!StreamIndexToDataSlices_[rhsIndex].empty());
            const auto& rhsDataSlice = StreamIndexToDataSlices_[rhsIndex].front();
            return ForeignComparator_.CompareKeyBounds(lhsDataSlice->UpperLimit().KeyBound, rhsDataSlice->UpperLimit().KeyBound) < 0;
        };
    }

    void AddToHeap(int streamIndex)
    {
        StreamHeap_.push_back(streamIndex);
        AdjustHeapBack(StreamHeap_.begin(), StreamHeap_.end(), GetStreamComparator());
    }

    void PopFromHeap()
    {
        ExtractHeap(StreamHeap_.begin(), StreamHeap_.end(), GetStreamComparator());
        StreamHeap_.pop_back();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! This class is responsible for holding the current "working set" of data slices.
class TSortedStagingArea final
    : public ISortedStagingArea
{
public:
    TSortedStagingArea(
        bool enableKeyGuarantee,
        TComparator primaryComparator,
        TComparator foreignComparator,
        TRowBufferPtr rowBuffer,
        const TLogger& logger)
        : PrimaryComparator_(std::move(primaryComparator))
        , ForeignComparator_(std::move(foreignComparator))
        , RowBuffer_(std::move(rowBuffer))
        , Logger(logger)
        , MainDomain_("Main", logger, PrimaryComparator_)
        , BufferDomain_("Buffer", logger, PrimaryComparator_)
        , ForeignDomain_(ForeignComparator_)
        , SolidDomain_(!enableKeyGuarantee
            ? std::make_optional<TPrimaryDomain>("Solid", Logger, PrimaryComparator_)
            : std::nullopt)
    {
        YT_LOG_DEBUG("Staging area instantiated");
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
                CurrentJobContainsSolidSlices_ = true;
                // TODO(apollo1321): Simplify this code later, see YT-26022.
                // YT_VERIFY for Buffer and Solid should be the same.

                // NB(apollo1321): Solid slice boundary matching rules:
                // - UpperBound "]" accepts solid slices with "[" or "(",
                // - UpperBound ")" accepts only solid slices with "[".
                YT_VERIFY(
                    dataSlice->LowerLimit().KeyBound == UpperBound_.Invert() ||
                    (dataSlice->LowerLimit().KeyBound.IsInclusive &&
                    dataSlice->LowerLimit().KeyBound == UpperBound_.Invert().ToggleInclusiveness()));
                YT_VERIFY(SolidDomain_.has_value());
                SolidDomain_->PushBack(std::move(dataSlice));
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

    TCurrentJobsStatistics Flush() override
    {
        // If we have no Main nor Solid slices, we have nothing to do.
        if (IsExhausted()) {
            YT_LOG_TRACE("Nothing to flush in staging area");
            // Nothing to flush.
            return JobsStatistics_;
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
        YT_VERIFY(MainDomain_.IsEmpty());

        return JobsStatistics_;
    }

    std::pair<std::vector<TNewJobStub>, TCurrentJobsStatistics> Finish() && override
    {
        YT_LOG_TRACE("Finishing work in staging area");

        PromoteUpperBound(TKeyBound::MakeUniversal(/*isUpper*/ true));

        Flush();
        for (const auto& domain : {MainDomain_, BufferDomain_}) {
            YT_VERIFY(domain.IsEmpty());
        }
        YT_VERIFY(!SolidDomain_.has_value() || SolidDomain_->IsEmpty());
        return {std::move(PreparedJobs_), JobsStatistics_};
    }

    TKeyBound GetPrimaryUpperBound() const override
    {
        TKeyBound upperBound = UpperBound_;
        auto updateUpperBound = [&] (auto domainUpperBound) {
            if (domainUpperBound) {
                upperBound = PrimaryComparator_.WeakerKeyBound(upperBound, domainUpperBound);
            }
        };
        updateUpperBound(MainDomain_.GetUpperBound());
        updateUpperBound(BufferDomain_.GetUpperBound());
        if (SolidDomain_.has_value()) {
            updateUpperBound(SolidDomain_->GetUpperBound());
        }
        return upperBound;
    }

    TResourceVector GetForeignResourceVector() const override
    {
        return ForeignDomain_.GetStatistics();
    }

private:
    const TComparator PrimaryComparator_;
    const TComparator ForeignComparator_;
    const TRowBufferPtr RowBuffer_;
    TLogger Logger;

    //! Upper bound using which all data slices in Main domain are to be cut.
    //! NB: Actual upper bound of job to be built may differ from #UpperBound_
    //! in case when singleton data slices are added to the job; in this case
    //! actual upper bound for a job will be #UpperBound_.ToggleInclusiveness()
    //! (i.e. exclusive instead of inclusive).
    TKeyBound UpperBound_ = TKeyBound::MakeEmpty(/*isUpper*/ true);

    TCurrentJobsStatistics JobsStatistics_;
    std::vector<TNewJobStub> PreparedJobs_;

    //! These flags are used only for internal sanity check.
    bool PreviousJobContainedSolidSlices_ = false;
    bool CurrentJobContainsSolidSlices_ = false;

    //! Previous job upper bound, used for internal sanity check.
    TKeyBound PreviousJobUpperBound_ = TKeyBound::MakeEmpty(/*isUpper*/ true);

    TPrimaryDomain MainDomain_;
    TPrimaryDomain BufferDomain_;
    TForeignDomain ForeignDomain_;
    std::optional<TPrimaryDomain> SolidDomain_;

    std::vector<TLegacyDataSlicePtr> NonSolidMainDataSlices_;

    TString GetStatisticsDebugString() const
    {
        return Format(
            "{Main: %v, Buffer: %v, Solid: %v}",
            MainDomain_.GetStatistics(),
            BufferDomain_.GetStatistics(),
            SolidDomain_.has_value() ? std::optional(SolidDomain_->GetStatistics()) : std::nullopt);
    }

    //! Check if we have at least one data slice to build job right now.
    bool IsExhausted() const
    {
        return MainDomain_.IsEmpty() && (!SolidDomain_.has_value() || SolidDomain_->IsEmpty());
    }

    void CutNonSolidMainByUpperBound()
    {
        YT_LOG_TRACE("Cutting non-solid main domain by upper bound (UpperBound: %v)", UpperBound_);

        // We first collect data slice to push to buffer domain, and only then push them.
        // We have to preserve the order of the slices from `NonSolidMainDataSlices_` and
        // push them into `SolidDomain_` or `BufferDomain_` before the slices that are already there.
        // So we have to use `PushFront()` in line (1) below for the slices to be moved.
        // Since this reverses the order of the moved slices, we also have to iterate over `toBuffer`
        // in reverse order in line (2) while preserving their relative oder in line (3).
        // Refer to explanation in YT-14566 for more details.
        std::vector<TLegacyDataSlicePtr> toBuffer;

        for (auto& dataSlice : NonSolidMainDataSlices_) {
            YT_VERIFY(!PrimaryComparator_.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound));
            YT_VERIFY(PrimaryComparator_.CompareKeyBounds(dataSlice->LowerLimit().KeyBound, UpperBound_) <= 0);

            if (!PrimaryComparator_.IsRangeEmpty(UpperBound_.Invert(), dataSlice->UpperLimit().KeyBound)) {
                // Right part of the data slice goes to Buffer domain.
                auto restDataSlice = CreateInputDataSlice(dataSlice, PrimaryComparator_, UpperBound_.Invert(), dataSlice->UpperLimit().KeyBound);
                // It may happen that data slice is entirely inside current upper bound (e.g. slice E from example 1 above), so
                // check if rest data slice is non-empty.
                if (!PrimaryComparator_.IsRangeEmpty(restDataSlice->LowerLimit().KeyBound, restDataSlice->UpperLimit().KeyBound)) {
                    restDataSlice->CopyPayloadFrom(*dataSlice);
                    toBuffer.emplace_back(std::move(restDataSlice)); // line (3)
                }
                // Left part of the data slice resides in the Main domain.
                dataSlice->UpperLimit().KeyBound = UpperBound_;
            }

            YT_VERIFY(PrimaryComparator_.CompareKeyBounds(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound) <= 0);
        }

        NonSolidMainDataSlices_.clear();

        for (auto& dataSlice : Reversed(toBuffer)) { // line (2)
            auto& destinationDomain = PrimaryComparator_.IsInteriorEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound) && SolidDomain_.has_value()
                ? *SolidDomain_
                : BufferDomain_;
            destinationDomain.PushFront(std::move(dataSlice)); // line (1)
        }
    }

    void TransferBufferToMain()
    {
        // NB: It is important to transfer singletons before non-singletons;
        // otherwise we would violate slice order guarantee.
        for (auto& dataSlice : BufferDomain_.ExtractDataSlicesAndClear()) {
            YT_VERIFY(dataSlice->LowerLimit().KeyBound == UpperBound_.Invert());
            NonSolidMainDataSlices_.push_back(dataSlice);
            MainDomain_.PushBack(std::move(dataSlice));
        }
    }

    void TransferSolidToMain()
    {
        if (!SolidDomain_.has_value()) {
            return;
        }
        for (auto& dataSlice : SolidDomain_->ExtractDataSlicesAndClear()) {
            YT_VERIFY(
                dataSlice->LowerLimit().KeyBound == UpperBound_.Invert() ||
                (dataSlice->LowerLimit().KeyBound.IsInclusive &&
                dataSlice->LowerLimit().KeyBound == UpperBound_.Invert().ToggleInclusiveness()));
            MainDomain_.PushBack(std::move(dataSlice));
        }
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
            if (CurrentJobContainsSolidSlices_) {
                // Remove this branch and CurrentJobContainsSolidSlices_ after "Weird max42 case"
                // is removed, see YT-26022.

                // NB(apollo1321): Solid slice with lower bound ">= K" can be added even if staging area
                // upper boundary is "<= K". As bound ordering logic requires that '>= K' < '<= K', we
                // need to handle this case manually.
                YT_VERIFY(
                    PrimaryComparator_.CompareKeyBounds(actualLowerBound, PreviousJobUpperBound_) >= 0 ||
                    actualLowerBound == PreviousJobUpperBound_.Invert().ToggleInclusiveness());
            } else {
                YT_VERIFY(PrimaryComparator_.CompareKeyBounds(actualLowerBound, PreviousJobUpperBound_) >= 0);
            }
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

    void DoFlush()
    {
        YT_VERIFY(!IsExhausted());

        auto& job = PreparedJobs_.emplace_back();

        YT_LOG_TRACE(
            "Flushing main and solid domains into job (MainStatistics: %v, SolidStatistics: %v)",
            MainDomain_.GetStatistics(),
            SolidDomain_.has_value() ? SolidDomain_->GetStatistics() : TResourceVector());

        // Calculate the actual lower and upper bounds of newly formed job and move data slices to the job.
        auto actualLowerBound = TKeyBound::MakeEmpty(/*isUpper*/ false);
        auto actualUpperBound = TKeyBound::MakeEmpty(/*isUpper*/ true);

        auto flushDomain = [&] (auto& domain) {
            for (auto& dataSlice : domain.ExtractDataSlicesAndClear()) {
                actualLowerBound = PrimaryComparator_.WeakerKeyBound(dataSlice->LowerLimit().KeyBound, actualLowerBound);
                actualUpperBound = PrimaryComparator_.WeakerKeyBound(dataSlice->UpperLimit().KeyBound, actualUpperBound);
                YT_VERIFY(dataSlice->Tag);
                const auto& Logger = domain.GetLogger();
                if (!PrimaryComparator_.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound)) {
                    YT_LOG_TRACE(
                        "Adding primary data slice to job (DataSlice: %v)",
                        GetDataSliceDebugString(dataSlice));
                    auto tag = *dataSlice->Tag;
                    job.AddDataSlice(std::move(dataSlice), tag, /*isPrimary*/ true);
                } else {
                    YT_LOG_TRACE(
                        "Not adding empty data slice to job (DataSlice: %v)",
                        GetDataSliceDebugString(dataSlice));
                }
            }
        };

        flushDomain(MainDomain_);
        if (SolidDomain_.has_value()) {
            flushDomain(*SolidDomain_);
        }

        if (job.GetPrimarySliceCount() == 0) {
            YT_VERIFY(PrimaryComparator_.IsRangeEmpty(actualLowerBound, actualUpperBound));
            YT_LOG_TRACE("Dropping empty job (DataSlices: %v)", job.GetDebugString());
            PreparedJobs_.pop_back();
            return;
        }
        job.SetPrimaryLowerBound(actualLowerBound);
        job.SetPrimaryUpperBound(actualUpperBound);

        // Perform sanity checks and prepare information for the next sanity check.
        ValidateCurrentJobBounds(actualLowerBound, actualUpperBound);
        PreviousJobUpperBound_ = UpperBound_;
        PreviousJobContainedSolidSlices_ = CurrentJobContainsSolidSlices_;
        CurrentJobContainsSolidSlices_ = false;

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
        for (const auto& dataSlices : ForeignDomain_.StreamIndexToDataSlices()) {
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

        ++JobsStatistics_.JobCount;
        JobsStatistics_.DataSliceCount += job.GetSliceCount();
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISortedStagingArea> CreateSortedStagingArea(
    bool enableKeyGuarantee,
    TComparator primaryComparator,
    TComparator foreignComparator,
    TRowBufferPtr rowBuffer,
    TLogger logger)
{
    return std::make_unique<TSortedStagingArea>(
        enableKeyGuarantee,
        std::move(primaryComparator),
        std::move(foreignComparator),
        std::move(rowBuffer),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
