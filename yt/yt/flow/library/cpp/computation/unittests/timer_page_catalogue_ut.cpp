#include <yt/yt/flow/library/cpp/computation/stores/timer_page_catalogue.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/timer_pages.h>

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/suspendable_invoker.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/generic/scope.h>

#include <algorithm>
#include <atomic>
#include <functional>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTimerPageCatalogueTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("comp");
    const TStreamId StreamId = TStreamId("timer");

    TActionQueuePtr Queue_ = New<TActionQueue>("Test");
    //! Runs the apply rounds of the catalogue.
    ISuspendableInvokerPtr CatalogueInvoker_ = CreateSuspendableInvoker(Queue_->GetInvoker());
    NTables::TInMemoryTimerPagesPtr Tables_ = New<NTables::TInMemoryTimerPages>();
    TTimerPageCataloguePtr Catalogue_;

    void TearDown() override
    {
        // The hooks capture the locals of the test body, which the applier may still reach.
        Tables_->SetHook({});
        if (Catalogue_) {
            WaitFor(Catalogue_->Stop()).ThrowOnError();
        }
        Queue_->Shutdown();
    }

    TTimerPageCataloguePtr CreateCatalogue(TTimerPageCatalogueOptions options = {})
    {
        options.EmptyPageCollectionPeriod = std::nullopt;
        Catalogue_ = New<TTimerPageCatalogue>(
            Tables_,
            [] {
                return MakeFuture(NApi::ITransactionPtr());
            },
            CatalogueInvoker_,
            NLogging::TLogger("Test"),
            NProfiling::TProfiler(),
            options);
        return Catalogue_;
    }

    //! Runs |action| while the apply rounds of the catalogue are held: a round reads the tables
    //! either before |action| or after it.
    void WithoutApplyRounds(const std::function<void()>& action)
    {
        // A round goes on after it completes a synchronization, and the periodic ones come at
        // any time. A round in progress is held at its next wait, after the table call it waits
        // for.
        WaitFor(CatalogueInvoker_->Suspend()).ThrowOnError();
        auto resume = Finally([this] {
            CatalogueInvoker_->Resume();
        });
        action();
    }

    TTimerPageMeta MakePage(ui64 familyId, ui64 beginTimestamp, std::optional<ui64> lower, std::optional<ui64> upper, i64 count = 0)
    {
        return TTimerPageMeta{
            .ComputationId = ComputationId,
            .FamilyId = TTimerFamilyId(familyId),
            .StreamId = StreamId,
            .KeyRange = MakeUintKeyRange(lower, upper),
            .Kind = ETimerPageKind::Trigger,
            .Begin = TSystemTimestamp(beginTimestamp),
            .Count = count,
        };
    }

    void WritePage(const TTimerPageMeta& page, bool journal = true)
    {
        Tables_->WritePage(nullptr, page);
        if (journal) {
            Tables_->WriteJournal(nullptr, {TTimerJournalEntry::Create(page)});
        }
    }

    static std::vector<ui64> Begins(const std::vector<TTimerCataloguePage>& pages)
    {
        std::vector<ui64> result;
        for (const auto& page : pages) {
            result.push_back(page.Begin.Underlying());
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTimerPageCatalogueTest, ClassifyPage)
{
    auto range = MakeUintKeyRange(10, 20);
    auto classify = [&] (std::optional<ui64> lower, std::optional<ui64> upper, bool onceShared = false) {
        return ClassifyTimerPage(MakeUintKeyRange(lower, upper), onceShared, range);
    };
    EXPECT_EQ(classify(10, 20), ETimerPageClass::Canonical);
    EXPECT_EQ(classify(10, 20, /*onceShared*/ true), ETimerPageClass::Nested);
    EXPECT_EQ(classify(12, 18), ETimerPageClass::Nested);
    EXPECT_EQ(classify(10, 15), ETimerPageClass::Nested);
    EXPECT_EQ(classify(15, 20), ETimerPageClass::Nested);
    EXPECT_EQ(classify(5, 15), ETimerPageClass::Shared);
    EXPECT_EQ(classify(15, 25), ETimerPageClass::Shared);
    EXPECT_EQ(classify(0, std::nullopt), ETimerPageClass::Shared);
    EXPECT_EQ(ClassifyTimerPage(UniversalKeyRange(), false, UniversalKeyRange()), ETimerPageClass::Canonical);
}

TEST_F(TTimerPageCatalogueTest, SnapshotQueries)
{
    auto catalogue = CreateCatalogue();
    std::vector<TTimerPageMeta> pages{
        MakePage(1, 11, 0, 10),
        MakePage(2, 22, 10, 20),
        MakePage(2, 21, 10, 20),
        MakePage(3, 31, 20, std::nullopt),
        MakePage(4, 41, 5, 25),
    };
    auto eventPage = MakePage(2, 23, 10, 20);
    eventPage.Kind = ETimerPageKind::Event;
    pages.push_back(eventPage);
    auto otherStream = MakePage(5, 51, 0, std::nullopt);
    otherStream.StreamId = TStreamId("other");
    pages.push_back(otherStream);
    for (const auto& page : pages) {
        WritePage(page);
    }
    WaitFor(catalogue->Synchronize()).ThrowOnError();

    auto snapshot = catalogue->GetSnapshot();
    EXPECT_EQ(snapshot.GetPageCount(), 7);
    EXPECT_EQ(Begins(snapshot.FindPages(ComputationId, StreamId, MakeUintKeyRange(10, 20))), (std::vector<ui64>{41, 21, 22, 23}));
    EXPECT_EQ(Begins(snapshot.FindPages(ComputationId, StreamId, MakeUintKeyRange(12, 15))), (std::vector<ui64>{41, 21, 22, 23}));
    EXPECT_EQ(Begins(snapshot.FindPages(ComputationId, StreamId, MakeUintKeyRange(0, 5))), (std::vector<ui64>{11}));
    EXPECT_EQ(Begins(snapshot.FindPages(ComputationId, StreamId, MakeUintKeyRange(25, std::nullopt))), (std::vector<ui64>{31}));
    EXPECT_EQ(Begins(snapshot.FindPages(ComputationId, StreamId, MakeUintKeyRange(0, std::nullopt))), (std::vector<ui64>{11, 41, 21, 22, 23, 31}));
    EXPECT_EQ(Begins(snapshot.FindPages(ComputationId, TStreamId("other"), MakeUintKeyRange(10, 20))), (std::vector<ui64>{51}));
    EXPECT_TRUE(snapshot.FindPages(TComputationId("none"), StreamId, MakeUintKeyRange(0, std::nullopt)).empty());

    EXPECT_EQ(Begins(snapshot.GetFamilyPages(TTimerFamilyId(2), ETimerPageKind::Trigger)), (std::vector<ui64>{21, 22}));
    EXPECT_EQ(Begins(snapshot.GetFamilyPages(TTimerFamilyId(2), ETimerPageKind::Event)), (std::vector<ui64>{23}));
    EXPECT_TRUE(snapshot.GetFamilyPages(TTimerFamilyId(9), ETimerPageKind::Trigger).empty());
    EXPECT_EQ(snapshot.FindPage(TTimerFamilyId(2), ETimerPageKind::Trigger, TSystemTimestamp(22)), TTimerCataloguePage::FromMeta(pages[1]));
    EXPECT_FALSE(snapshot.FindPage(TTimerFamilyId(2), ETimerPageKind::Event, TSystemTimestamp(22)));

    // A snapshot is unaffected by later changes.
    Tables_->WriteJournal(nullptr, {TTimerJournalEntry::Delete(pages[1])});
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 6);
    EXPECT_EQ(snapshot.GetPageCount(), 7);
}

TEST_F(TTimerPageCatalogueTest, BootstrapAndApply)
{
    WritePage(MakePage(1, 11, 0, 10));
    WritePage(MakePage(1, 12, 0, 10));

    auto catalogue = CreateCatalogue();
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 2);
    i64 version = catalogue->GetVersion();

    // Entries already covered by the snapshot are applied again without effect.
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetVersion(), version);

    // New journal entries become visible after a synchronization.
    auto page = MakePage(2, 21, 10, 20);
    WritePage(page);
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 3);
    EXPECT_GT(catalogue->GetVersion(), version);

    Tables_->WriteJournal(nullptr, {
            TTimerJournalEntry::SetFlags(page, {.OnceShared = true}),
            TTimerJournalEntry::SetFlags(page, {.EventDone = true}),
            TTimerJournalEntry::Delete(MakePage(1, 12, 0, 10)),
                                   });
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    auto snapshot = catalogue->GetSnapshot();
    auto found = snapshot.FindPage(TTimerFamilyId(2), ETimerPageKind::Trigger, TSystemTimestamp(21));
    ASSERT_TRUE(found);
    // Both flag changes apply; the second does not reset the first.
    EXPECT_TRUE(found->OnceShared);
    EXPECT_TRUE(found->EventDone);
    EXPECT_FALSE(found->DedupSlots);
    EXPECT_FALSE(snapshot.FindPage(TTimerFamilyId(1), ETimerPageKind::Trigger, TSystemTimestamp(12)));
    EXPECT_EQ(snapshot.GetPageCount(), 2);
}

TEST_F(TTimerPageCatalogueTest, EntriesApplyInJournalOrderAcrossBatches)
{
    // The journal is the only writer of the snapshot: entries that arrive one per read apply
    // in their order, so a sticky flag set after the creation of its page stays set.
    auto page = MakePage(1, 11, 0, 10);
    auto other = MakePage(2, 21, 10, 20);
    auto catalogue = CreateCatalogue({.JournalReadLimit = 1});
    WaitFor(catalogue->Synchronize()).ThrowOnError();

    WritePage(page);
    WritePage(other);
    Tables_->WriteJournal(nullptr, {
            TTimerJournalEntry::SetFlags(page, {.OnceShared = true}),
            TTimerJournalEntry::Delete(other),
                                   });
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    auto snapshot = catalogue->GetSnapshot();
    EXPECT_TRUE(snapshot.FindPage(TTimerFamilyId(1), ETimerPageKind::Trigger, TSystemTimestamp(11))->OnceShared);
    EXPECT_FALSE(snapshot.FindPage(TTimerFamilyId(2), ETimerPageKind::Trigger, TSystemTimestamp(21)));

    // A bootstrap overlap delivers an older entry again; it is applied as the state of its time
    // only when nothing newer follows, which the snapshot read already reflects.
    page.Flags.OnceShared = true;
    Tables_->WritePage(nullptr, page);
    Tables_->WriteJournal(nullptr, {TTimerJournalEntry::Create(page)});
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_TRUE(catalogue->GetSnapshot().FindPage(TTimerFamilyId(1), ETimerPageKind::Trigger, TSystemTimestamp(11))->OnceShared);
}

TEST_F(TTimerPageCatalogueTest, BootstrapReplaysTheOverlapPrivately)
{
    // Between the row count a bootstrap takes first and its metadata read, a page is created
    // and flagged sticky, and another page is deleted: the metadata already reflects both.
    // With one entry per read the older Create and the deleted page's Create replay after the
    // metadata, but privately: no snapshot anybody observes shows the flag reset or the page
    // back.
    auto page = MakePage(1, 11, 0, 10);
    auto gone = MakePage(2, 21, 10, 20);
    auto catalogue = CreateCatalogue({.JournalReadLimit = 1});

    std::vector<bool> flagObservedReset;
    std::vector<bool> goneObserved;
    bool written = false;
    auto stopCatalogue = Finally([this] {
        Tables_->SetHook({});
        if (Catalogue_) {
            WaitFor(Catalogue_->Stop()).ThrowOnError();
        }
    });
    Tables_->SetHook([&] (TStringBuf method) {
        if (method == "LoadMeta" && !written) {
            written = true;
            WritePage(page);
            WritePage(gone);
            page.Flags.OnceShared = true;
            Tables_->WritePage(nullptr, page);
            Tables_->WriteJournal(nullptr, {TTimerJournalEntry::SetFlags(page, {.OnceShared = true})});
            Tables_->ErasePage(nullptr, gone.ComputationId, gone.FamilyId, gone.Kind, gone.Begin);
            Tables_->WriteJournal(nullptr, {TTimerJournalEntry::Delete(gone)});
        }
        if (method == "ReadJournal") {
            auto snapshot = catalogue->GetSnapshot();
            if (auto found = snapshot.FindPage(page.FamilyId, page.Kind, page.Begin)) {
                flagObservedReset.push_back(!found->OnceShared);
            }
            goneObserved.push_back(snapshot.FindPage(gone.FamilyId, gone.Kind, gone.Begin).has_value());
        }
    });

    WaitFor(catalogue->Synchronize()).ThrowOnError();
    Tables_->SetHook({});
    WaitFor(catalogue->Stop()).ThrowOnError();
    EXPECT_TRUE(written);
    auto snapshot = catalogue->GetSnapshot();
    EXPECT_TRUE(snapshot.FindPage(page.FamilyId, page.Kind, page.Begin)->OnceShared);
    EXPECT_FALSE(snapshot.FindPage(gone.FamilyId, gone.Kind, gone.Begin));
    // Four entries replayed one by one: the observations cover the intermediate states.
    EXPECT_GE(goneObserved.size(), 4u);
    EXPECT_EQ(std::count(flagObservedReset.begin(), flagObservedReset.end(), true), 0);
    EXPECT_EQ(std::count(goneObserved.begin(), goneObserved.end(), true), 0);
}

TEST_F(TTimerPageCatalogueTest, TrimDuringBootstrapDoesNotPublishPartialState)
{
    // The overlap holds Create(once_shared = false) and SetFlags(true); a trim after the first
    // private replay batch leaves the second unreadable. The half replayed state must not be
    // published: the next round bootstraps again from fresh metadata.
    auto page = MakePage(1, 11, 0, 10);
    auto catalogue = CreateCatalogue({.JournalReadLimit = 1});
    std::atomic<bool> badSnapshot = false;
    int reads = 0;
    bool written = false;
    auto stopCatalogue = Finally([this] {
        Tables_->SetHook({});
        if (Catalogue_) {
            WaitFor(Catalogue_->Stop()).ThrowOnError();
        }
    });
    Tables_->SetHook([&] (TStringBuf method) {
        if (method == "LoadMeta" && !written) {
            written = true;
            WritePage(page);
            page.Flags.OnceShared = true;
            Tables_->WritePage(nullptr, page);
            Tables_->WriteJournal(nullptr, {TTimerJournalEntry::SetFlags(page, {.OnceShared = true})});
        }
        if (method == "ReadJournal" && ++reads == 2) {
            Tables_->TrimJournal(2);
        }
        if (method == "GetJournalRowRange") {
            auto found = catalogue->GetSnapshot().FindPage(page.FamilyId, page.Kind, page.Begin);
            if (found && !found->OnceShared) {
                badSnapshot.store(true);
            }
        }
    });
    WaitFor(catalogue->Synchronize().WithTimeout(TDuration::Seconds(30))).ThrowOnError();
    Tables_->SetHook({});
    WaitFor(catalogue->Stop()).ThrowOnError();
    EXPECT_TRUE(written);
    EXPECT_FALSE(badSnapshot.load());
    EXPECT_TRUE(catalogue->GetSnapshot().FindPage(page.FamilyId, page.Kind, page.Begin)->OnceShared);
}

TEST_F(TTimerPageCatalogueTest, JournalReadInChunks)
{
    for (i64 index = 1; index <= 7; ++index) {
        WritePage(MakePage(index, 10 * index, index, index + 1));
    }
    auto catalogue = CreateCatalogue({.JournalReadLimit = 2});
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 7);
    for (i64 index = 8; index <= 12; ++index) {
        WritePage(MakePage(index, 10 * index, index, index + 1));
    }
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 12);
}

TEST_F(TTimerPageCatalogueTest, TrimmedJournalBootstrapsAgain)
{
    WritePage(MakePage(1, 11, 0, 10));
    auto catalogue = CreateCatalogue();
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 1);

    // The journal is trimmed past the applied position, which stays after the first entry. A page
    // written to the metadata without a journal entry is only seen by a bootstrap.
    WithoutApplyRounds([&] {
        WritePage(MakePage(2, 21, 10, 20), /*journal*/ false);
        WritePage(MakePage(3, 31, 20, 30));
        WritePage(MakePage(4, 41, 30, 40));
        Tables_->TrimJournal(2);
    });
    WaitFor(catalogue->Synchronize().WithTimeout(TDuration::Seconds(30))).ThrowOnError();
    auto snapshot = catalogue->GetSnapshot();
    EXPECT_EQ(snapshot.GetPageCount(), 4);
    EXPECT_TRUE(snapshot.FindPage(TTimerFamilyId(2), ETimerPageKind::Trigger, TSystemTimestamp(21)));
}

TEST_F(TTimerPageCatalogueTest, TrimNotPastTheAppliedPositionDoesNotBootstrap)
{
    // With the collection off, only a bootstrap reads the metadata.
    auto metadataReads = std::make_shared<std::atomic<int>>(0);
    Tables_->SetHook([metadataReads] (TStringBuf method) {
        if (method == "LoadMeta") {
            ++*metadataReads;
        }
    });
    // The journal is trimmed to its end before the bootstrap, so the applied position starts at
    // the trim; the round that bootstraps checks the trim again before it applies anything else.
    WritePage(MakePage(1, 11, 0, 10));
    WritePage(MakePage(2, 21, 10, 20));
    Tables_->TrimJournal(2);
    auto catalogue = CreateCatalogue({.JournalReadLimit = 1});
    WaitFor(catalogue->Synchronize()).ThrowOnError();

    // The whole journal is applied before the next trim, as a background round may do: no entry
    // is lost, so no bootstrap reveals the page written to the metadata without a journal entry.
    WritePage(MakePage(3, 31, 20, 30), /*journal*/ false);
    WritePage(MakePage(4, 41, 30, 40));
    WritePage(MakePage(5, 51, 40, 50));
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    Tables_->TrimJournal(3);
    // The entries written after the trim make a round check it before the synchronization
    // completes: a round that read the row range before the trim applies only one of them.
    WritePage(MakePage(6, 61, 50, 60));
    WritePage(MakePage(7, 71, 60, 70));
    WaitFor(catalogue->Synchronize().WithTimeout(TDuration::Seconds(30))).ThrowOnError();

    auto snapshot = catalogue->GetSnapshot();
    EXPECT_EQ(Begins(snapshot.GetPages()), (std::vector<ui64>{11, 21, 41, 51, 61, 71}));
    EXPECT_EQ(metadataReads->load(), 1);
}

TEST_F(TTimerPageCatalogueTest, TrimToTheEndBootstrapsAgain)
{
    WritePage(MakePage(1, 11, 0, 10));
    auto catalogue = CreateCatalogue();
    WaitFor(catalogue->Synchronize()).ThrowOnError();

    // Everything after the applied position is trimmed away and nothing follows: a read from
    // there is empty, yet the catalogue must catch up and the synchronization must complete.
    WithoutApplyRounds([&] {
        WritePage(MakePage(2, 21, 10, 20));
        WritePage(MakePage(3, 31, 20, 30));
        auto total = WaitFor(Tables_->GetJournalRowRange()).ValueOrThrow().TotalRowCount;
        Tables_->TrimJournal(total);
    });
    WaitFor(catalogue->Synchronize().WithTimeout(TDuration::Seconds(30))).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 3);
}

TEST_F(TTimerPageCatalogueTest, CollectEmptyPages)
{
    // Once shared: an empty page goes, a page with rows and a page with a count stay.
    auto shared = [&] (ui64 familyId, ui64 beginTimestamp, i64 count) {
        auto page = MakePage(familyId, beginTimestamp, 0, 10, count);
        page.Flags.OnceShared = true;
        return page;
    };
    auto first = shared(1, 0, 0);
    first.End = TSystemTimestamp(10);
    WritePage(first);
    auto middle = shared(1, 10, 0);
    middle.End = TSystemTimestamp(20);
    WritePage(middle);
    WritePage(shared(1, 20, 5));
    TTimer timer;
    timer.MessageId = TMessageId("id");
    timer.TriggerTimestamp = TSystemTimestamp(25);
    timer.Key = MakeKey(1);
    Tables_->WriteRows(nullptr, {TTimerPageRow{
                .RowKey = MakeTimerPageRowKey(TTimerFamilyId(1), ETimerPageKind::Trigger, timer, false),
                .Timer = timer,
                                }});
    // Never shared: untouched however empty.
    WritePage(MakePage(3, 31, 20, 30));

    auto catalogue = CreateCatalogue();
    auto journalRows = WaitFor(Tables_->GetJournalRowRange()).ValueOrThrow().TotalRowCount;
    catalogue->CollectEmptyPages();
    WaitFor(catalogue->Synchronize()).ThrowOnError();

    auto snapshot = catalogue->GetSnapshot();
    EXPECT_EQ(Begins(snapshot.GetFamilyPages(TTimerFamilyId(1), ETimerPageKind::Trigger)), (std::vector<ui64>{0, 20}));
    EXPECT_EQ(snapshot.GetFamilyPages(TTimerFamilyId(3), ETimerPageKind::Trigger).size(), 1u);
    EXPECT_EQ(WaitFor(Tables_->LoadMeta({.ComputationId = ComputationId})).ValueOrThrow().size(), 3u);
    // One page deletion was journaled.
    EXPECT_EQ(WaitFor(Tables_->GetJournalRowRange()).ValueOrThrow().TotalRowCount, journalRows + 1);

    // A second pass finds nothing.
    catalogue->CollectEmptyPages();
    EXPECT_EQ(WaitFor(Tables_->GetJournalRowRange()).ValueOrThrow().TotalRowCount, journalRows + 1);
}

TEST_F(TTimerPageCatalogueTest, CollectionUsesCountWithoutReadingTimers)
{
    auto page = MakePage(1, 0, 0, 10, 0);
    page.Flags.OnceShared = true;
    WritePage(page);
    auto catalogue = CreateCatalogue();
    i64 reads = Tables_->GetPageReadCount();
    i64 aggregates = Tables_->GetAggregateCount();
    catalogue->CollectEmptyPages();
    EXPECT_TRUE(WaitFor(Tables_->LoadMeta({})).ValueOrThrow().empty());
    EXPECT_EQ(Tables_->GetPageReadCount(), reads);
    // One aggregate double-checks the emptiness of the collected page.
    EXPECT_EQ(Tables_->GetAggregateCount(), aggregates + 1);
}

TEST_F(TTimerPageCatalogueTest, CollectionKeepsPageWhoseCountUnderstatesRows)
{
    auto page = MakePage(1, 0, 0, 10, 0);
    page.Flags.OnceShared = true;
    WritePage(page);
    TTimer timer;
    timer.MessageId = TMessageId("id");
    timer.TriggerTimestamp = TSystemTimestamp(5);
    timer.Key = MakeKey(1);
    Tables_->WriteRows(nullptr, {TTimerPageRow{
                .RowKey = MakeTimerPageRowKey(TTimerFamilyId(1), ETimerPageKind::Trigger, timer, /*dedupSlots*/ false),
                .Timer = timer,
                                }});
    auto catalogue = CreateCatalogue();
    catalogue->CollectEmptyPages();
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(WaitFor(Tables_->LoadMeta({})).ValueOrThrow().size(), 1u);
    EXPECT_EQ(catalogue->GetSnapshot().GetFamilyPages(TTimerFamilyId(1), ETimerPageKind::Trigger).size(), 1u);
    EXPECT_EQ(Tables_->GetRowCount(), 1);
}

TEST_F(TTimerPageCatalogueTest, CollectsConsecutiveEmptyPages)
{
    for (auto kind : {ETimerPageKind::Trigger, ETimerPageKind::Event}) {
        for (i64 begin = 0; begin < 64; ++begin) {
            auto page = MakePage(1, begin, 0, 10);
            page.Kind = kind;
            page.Flags.OnceShared = true;
            if (begin != 63) {
                page.End = TSystemTimestamp(begin + 1);
            }
            WritePage(page);
        }
    }
    auto catalogue = CreateCatalogue();
    catalogue->CollectEmptyPages();
    auto pages = WaitFor(Tables_->LoadMeta({})).ValueOrThrow();
    ASSERT_EQ(pages.size(), 2u);
    for (const auto& page : pages) {
        EXPECT_EQ(page.Begin, TSystemTimestamp(0));
        EXPECT_FALSE(page.End);
    }
    catalogue->CollectEmptyPages();
    EXPECT_TRUE(WaitFor(Tables_->LoadMeta({})).ValueOrThrow().empty());
}

TEST_F(TTimerPageCatalogueTest, StopDuringSynchronization)
{
    auto catalogue = CreateCatalogue();
    auto reading = NewPromise<void>();
    auto resume = NewPromise<void>();
    auto cleanup = Finally([&] {
        resume.TrySet();
        Tables_->SetHook({});
        WaitFor(catalogue->Stop()).ThrowOnError();
    });
    Tables_->SetHook([reading, resume] (TStringBuf method) {
        if (method == "GetJournalRowRange") {
            reading.TrySet();
            WaitFor(resume.ToFuture()).ThrowOnError();
        }
    });
    auto synchronized = BIND([catalogue] {
        return catalogue->Synchronize();
    })
        .AsyncVia(Queue_->GetInvoker())
        .Run();
    WaitFor(reading.ToFuture()).ThrowOnError();
    WaitFor(catalogue->Stop()).ThrowOnError();
    resume.Set();
    EXPECT_FALSE(WaitFor(synchronized).IsOK());
    EXPECT_FALSE(WaitFor(catalogue->Synchronize()).IsOK());
    EXPECT_EQ(catalogue->GetVersion(), 0);
}

TEST_F(TTimerPageCatalogueTest, KeyBoundsAreInterned)
{
    // Pages of two families share the bounds 10 and 20; the catalogue holds each bound once.
    WritePage(MakePage(1, 11, 0, 10));
    WritePage(MakePage(1, 12, 0, 10));
    WritePage(MakePage(2, 21, 10, 20));
    WritePage(MakePage(3, 31, 10, 20));
    auto catalogue = CreateCatalogue();
    WaitFor(catalogue->Synchronize()).ThrowOnError();

    auto snapshot = catalogue->GetSnapshot();
    auto sameObject = [] (const TKey& lhs, const TKey& rhs) {
        return lhs.Underlying().Begin() == rhs.Underlying().Begin();
    };
    auto page11 = *snapshot.FindPage(TTimerFamilyId(1), ETimerPageKind::Trigger, TSystemTimestamp(11));
    auto page12 = *snapshot.FindPage(TTimerFamilyId(1), ETimerPageKind::Trigger, TSystemTimestamp(12));
    auto page21 = *snapshot.FindPage(TTimerFamilyId(2), ETimerPageKind::Trigger, TSystemTimestamp(21));
    auto page31 = *snapshot.FindPage(TTimerFamilyId(3), ETimerPageKind::Trigger, TSystemTimestamp(31));
    EXPECT_TRUE(sameObject(page11.KeyRange.Lower, page12.KeyRange.Lower));
    EXPECT_TRUE(sameObject(page11.KeyRange.Upper, page21.KeyRange.Lower));
    EXPECT_TRUE(sameObject(page21.KeyRange.Upper, page31.KeyRange.Upper));

    // A page applied from the journal later shares the bounds too.
    WritePage(MakePage(4, 41, 20, 30));
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    auto page41 = *catalogue->GetSnapshot().FindPage(TTimerFamilyId(4), ETimerPageKind::Trigger, TSystemTimestamp(41));
    EXPECT_TRUE(sameObject(page41.KeyRange.Lower, page21.KeyRange.Upper));
}

TEST_F(TTimerPageCatalogueTest, PersistentFailureFailsSynchronization)
{
    WritePage(MakePage(1, 11, 0, 10));
    Tables_->SetHook([] (TStringBuf method) {
        if (method == "LoadMeta") {
            THROW_ERROR_EXCEPTION("Unreadable metadata");
        }
    });
    auto catalogue = CreateCatalogue({.JournalPollPeriod = TDuration::MilliSeconds(10), .FailedRoundsToFailWaiters = 3});
    auto error = WaitFor(catalogue->Synchronize());
    EXPECT_FALSE(error.IsOK());
    EXPECT_TRUE(error.FindMatching([] (const TError& inner) {
        return inner.GetMessage() == "Unreadable metadata";
    }));

    // Once the tables are readable again, the catalogue recovers.
    Tables_->SetHook({});
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 1);
}

TEST_F(TTimerPageCatalogueTest, StopFailsWaiters)
{
    Tables_->SetHook([] (TStringBuf method) {
        if (method == "LoadMeta") {
            THROW_ERROR_EXCEPTION("Unreadable metadata");
        }
    });
    auto catalogue = CreateCatalogue({.JournalPollPeriod = TDuration::Hours(1), .FailedRoundsToFailWaiters = 1000});
    auto synchronized = catalogue->Synchronize();
    WaitFor(catalogue->Stop()).ThrowOnError();
    Catalogue_.Reset();
    EXPECT_FALSE(WaitFor(synchronized).IsOK());
}

TEST_F(TTimerPageCatalogueTest, CollectionSurvivesAFailedSynchronization)
{
    auto page = MakePage(1, 11, 0, 10);
    page.Flags.OnceShared = true;
    WritePage(page);
    std::atomic<bool> fail = true;
    auto stopCatalogue = Finally([this] {
        Tables_->SetHook({});
        if (Catalogue_) {
            WaitFor(Catalogue_->Stop()).ThrowOnError();
        }
    });
    Tables_->SetHook([&] (TStringBuf method) {
        if (method == "LoadMeta" && fail.load()) {
            THROW_ERROR_EXCEPTION("Unreadable metadata");
        }
    });
    auto catalogue = CreateCatalogue({.JournalPollPeriod = TDuration::MilliSeconds(10), .FailedRoundsToFailWaiters = 1});
    // An escaping exception would stop the periodic collection for good.
    EXPECT_NO_THROW(catalogue->CollectEmptyPages());
    fail = false;
    catalogue->CollectEmptyPages();
    WaitFor(catalogue->Synchronize()).ThrowOnError();
    EXPECT_EQ(catalogue->GetSnapshot().GetPageCount(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
