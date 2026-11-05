#include <yt/yt/server/lib/hydra/unbuffered_file_changelog.h>
#include <yt/yt/server/lib/hydra/serialize.h>
#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/getopt/last_getopt.h>

namespace NYT {

using namespace NFS;
using namespace NHydra;
using namespace NIO;
using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "ChangelogSurgeonLogger");
constexpr int MinRecordsPerRead = 1;
constexpr int DefaultMaxRecordsPerRead = 1'000'000;
constexpr int ResultingChangelogMaxRecordCount = 100'000'000;

////////////////////////////////////////////////////////////////////////////////

struct TSurgeonParams
{
    std::optional<i64> FirstSequenceNumber;
    std::optional<i64> LastSequenceNumber;
    std::string ChangelogList;
    std::string ResultingChangelogName;
    int MaxRecordsPerRead = DefaultMaxRecordsPerRead;
};

////////////////////////////////////////////////////////////////////////////////

std::pair<i64, i64> GetSequenceNumberRange(const TVector<std::string>& changelogFileNames)
{
    bool changelogsEmpty = true;
    auto minSequenceNumber = std::numeric_limits<i64>::max();
    auto maxSequenceNumber = std::numeric_limits<i64>::min();

    auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, NYT::NYTree::INodePtr());
    auto config = New<TFileChangelogConfig>();

    YT_VERIFY(ssize(changelogFileNames) > 0);

    for (const auto& fileName : changelogFileNames) {
        auto currentChangelog = CreateUnbufferedFileChangelog(
            ioEngine,
            /*memoryUsageTracker*/ nullptr,
            fileName,
            config);
        currentChangelog->Open();

        auto recordCount = currentChangelog->GetRecordCount();
        if (recordCount == 0) {
            YT_TLOG_INFO("Changelog is empty")
                .With("FileName", fileName);
            continue;
        }
        changelogsEmpty = false;

        auto records = currentChangelog->Read(0, 1, std::numeric_limits<i64>::max());
        THROW_ERROR_EXCEPTION_IF(records.empty(), "No records were read from a changelog");
        YT_VERIFY(std::ssize(records) == 1);

        NYT::NHydra::NProto::TMutationHeader mutationHeader;
        TSharedRef mutationData;
        DeserializeMutationRecord(records[0], &mutationHeader, &mutationData);

        auto seqNumber = mutationHeader.sequence_number();
        minSequenceNumber = std::min(minSequenceNumber, seqNumber);
        maxSequenceNumber = std::max(maxSequenceNumber, seqNumber + recordCount - 1);
    }

    if (changelogsEmpty) {
        THROW_ERROR_EXCEPTION("All changelogs are empty");
    }

    YT_TLOG_INFO("Computed the records range")
        .With("MinSequenceNumber", minSequenceNumber)
        .With("MaxSequenceNumber", maxSequenceNumber);

    return {minSequenceNumber, maxSequenceNumber};
}

void ValidateSurgeonParams(TSurgeonParams* params)
{
    auto changelogFileNames = StringSplitter(params->ChangelogList).Split(' ').ToList<std::string>();
    auto [minSequenceNumber, maxSequenceNumber] = GetSequenceNumberRange(changelogFileNames);

    if (!params->FirstSequenceNumber.has_value()) {
        YT_TLOG_INFO("Deduced the first sequence number")
            .With("FirstSequenceNumber", minSequenceNumber);
        params->FirstSequenceNumber = minSequenceNumber;
    }

    if (!params->LastSequenceNumber.has_value()) {
        YT_TLOG_INFO("Deduced the last sequence number")
            .With("LastSequenceNumber", maxSequenceNumber);
        params->LastSequenceNumber = maxSequenceNumber;
    }

    if (params->FirstSequenceNumber < minSequenceNumber) {
        THROW_ERROR_EXCEPTION("First sequence number should be not less than %v", minSequenceNumber)
            .With("first_sequence_number", params->FirstSequenceNumber);
    }

    if (params->LastSequenceNumber > maxSequenceNumber) {
        THROW_ERROR_EXCEPTION("Last sequence number should be not greater than %v", maxSequenceNumber)
            .With("last_sequence_number", params->LastSequenceNumber);
    }

    if (params->FirstSequenceNumber > params->LastSequenceNumber) {
        THROW_ERROR_EXCEPTION("The first sequence number should be not greater than the last sequence number")
            .With("first_sequence_number", params->FirstSequenceNumber)
            .With("last_sequence_number", params->LastSequenceNumber);
    }

    if (params->MaxRecordsPerRead < MinRecordsPerRead || params->MaxRecordsPerRead > ResultingChangelogMaxRecordCount) {
        THROW_ERROR_EXCEPTION(
            "Max records per read should be in the range %v",
            Format("[%v, %v]", MinRecordsPerRead, ResultingChangelogMaxRecordCount))
            .With("max_records_per_read", params->MaxRecordsPerRead);
    }

    auto resultingChangelogRecordCount = maxSequenceNumber - minSequenceNumber + 1;
    if (resultingChangelogRecordCount > ResultingChangelogMaxRecordCount) {
        THROW_ERROR_EXCEPTION("Resulting changelog is too big")
            .With("resulting_changelog_record_count", resultingChangelogRecordCount)
            .With("resulting_changelog_max_record_count", resultingChangelogRecordCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateRecordIdentity(const TSharedRef& lhs, const TSharedRef& rhs)
{
    NYT::NHydra::NProto::TMutationHeader lhsMutationHeader;
    NYT::NHydra::NProto::TMutationHeader rhsMutationHeader;
    TSharedRef lhsMutationData;
    TSharedRef rhsMutationData;

    DeserializeMutationRecord(lhs, &lhsMutationHeader, &lhsMutationData);
    DeserializeMutationRecord(rhs, &rhsMutationHeader, &rhsMutationData);

    auto serializedLhsMutationHeader = ToString(lhsMutationHeader);
    auto serializedRhsMutationHeader = ToString(rhsMutationHeader);
    if (serializedLhsMutationHeader != serializedRhsMutationHeader) {
        THROW_ERROR_EXCEPTION("Mutation headers of mutations with the same sequence number differ")
            .With("first_mutation_header", serializedLhsMutationHeader)
            .With("second_mutation_header", serializedRhsMutationHeader);
    }

    if (!TRef::AreBitwiseEqual(lhsMutationData, rhsMutationData)) {
        THROW_ERROR_EXCEPTION("Mutation data of mutations with the same sequence number differ")
            .With("first_mutation_header", serializedLhsMutationHeader)
            .With("second_mutation_header", serializedRhsMutationHeader);
    }
}

void PerformSurgery(const TSurgeonParams& params)
{
    auto changelogFileNames = StringSplitter(params.ChangelogList).Split(' ').ToList<std::string>();

    auto firstSequenceNumber = *params.FirstSequenceNumber;
    auto lastSequenceNumber = *params.LastSequenceNumber;

    std::vector<TSharedRef> resultingRecords(lastSequenceNumber - firstSequenceNumber + 1);

    NYT::NHydra::NProto::TMutationHeader mutationHeader;
    TSharedRef mutationData;

    auto ioEngine = CreateIOEngine(EIOEngineType::ThreadPool, NYT::NYTree::INodePtr());
    auto config = New<TFileChangelogConfig>();

    for (const auto& fileName : changelogFileNames) {
        auto currentChangelog = CreateUnbufferedFileChangelog(
            ioEngine,
            /*memoryUsageTracker*/ nullptr,
            fileName,
            config);
        currentChangelog->Open();
        auto guard = Finally([=] {
            currentChangelog->Close();
        });

        auto recordCount = currentChangelog->GetRecordCount();
        if (recordCount == 0) {
            continue;
        }

        // Read a part of a changelog.
        auto recordsRead = 0;
        while (recordsRead < recordCount) {
            auto changelogRecords = currentChangelog->Read(recordsRead, params.MaxRecordsPerRead, std::numeric_limits<i64>::max());
            THROW_ERROR_EXCEPTION_IF(changelogRecords.size() == 0, "No records were read from a changelog");
            recordsRead += changelogRecords.size();

            for (const auto& record : changelogRecords) {
                DeserializeMutationRecord(record, &mutationHeader, &mutationData);
                auto sequenceNumber = mutationHeader.sequence_number();
                if (firstSequenceNumber <= sequenceNumber && sequenceNumber <= lastSequenceNumber) {
                    auto recordId = sequenceNumber - firstSequenceNumber;
                    if (resultingRecords[recordId].Empty()) {
                        resultingRecords[recordId] = record;
                    } else {
                        ValidateRecordIdentity(record, resultingRecords[recordId]);
                    }
                }
            }
        }
    }

    for (int i = 0; i < ssize(resultingRecords); ++i) {
        if (resultingRecords[i].Empty()) {
            THROW_ERROR_EXCEPTION("Mutation record is missing")
                .With("missing_sequence_number", firstSequenceNumber + i);
        }
    }

    auto resultingChangelogId = FromString<i32>(params.ResultingChangelogName);
    std::optional<NHydra::NProto::TMutationHeader> prevMutationHeader;
    for (int i = 0; i < ssize(resultingRecords); ++i) {
        DeserializeMutationRecord(resultingRecords[i], &mutationHeader, &mutationData);
        if (prevMutationHeader && mutationHeader.prev_random_seed() != prevMutationHeader->random_seed()) {
            THROW_ERROR_EXCEPTION("Random seeds do not match")
                .With("first_mutation_header", ToString(mutationHeader))
                .With("second_mutation_header", ToString(*prevMutationHeader));
        }

        if (mutationHeader.segment_id() != resultingChangelogId) {
            YT_TLOG_DEBUG("An original mutation segment_id was substituted with a new one")
                .With("OldSegmentId", mutationHeader.segment_id())
                .With("NewSegmentId", resultingChangelogId);
        }

        if (mutationHeader.record_id() != i) {
            YT_TLOG_DEBUG("An original mutation record_id was substituted with a new one")
                .With("OldRecordId", mutationHeader.record_id())
                .With("NewRecordId", i);
        }

        mutationHeader.set_segment_id(resultingChangelogId);
        mutationHeader.set_record_id(i);
        resultingRecords[i] = SerializeMutationRecord(mutationHeader, mutationData);
        prevMutationHeader = mutationHeader;
    }

    auto resultingChangelogName = params.ResultingChangelogName + ".log";
    auto resultingChangelog = CreateUnbufferedFileChangelog(
        ioEngine,
        /*memoryUsageTracker*/ nullptr,
        resultingChangelogName,
        config);
    resultingChangelog->Create(/*meta*/ {});
    resultingChangelog->Append(0, resultingRecords);
    resultingChangelog->Close();
}

void Run(int argc, char** argv)
{
    TSurgeonParams params;
    TOpts opts;

    opts.AddCharOption('f', "[inclusive] Start of the range of sequence numbers to be used to build a new changelog")
        .RequiredArgument("FIRST_SEQUENCE_NUMBER")
        .Optional()
        .StoreResult(&params.FirstSequenceNumber);

    opts.AddCharOption('l', "[inclusive] End of the range of sequence numbers to be used to build a new changelog")
        .RequiredArgument("LAST_SEQUENCE_NUMBER")
        .Optional()
        .StoreResult(&params.LastSequenceNumber);

    opts.AddCharOption('i', "Desired output changelog name, should be convertible to a number")
        .RequiredArgument("CHANGELOG_ID")
        .Required()
        .StoreResult(&params.ResultingChangelogName);

    opts.AddCharOption('c', "List of all changelogs that can be used to transplant their entries to the new one")
        .RequiredArgument("CHANGELOG_LIST")
        .Required()
        .StoreResult(&params.ChangelogList);

    opts.AddLongOption("max-records-per-read", "Max number of records to read from a changelog at once")
        .RequiredArgument("RECORDS_PER_READ")
        .StoreResult(&params.MaxRecordsPerRead);

    TOptsParseResult parseResult(&opts, argc, argv);

    try {
        ValidateSurgeonParams(&params);
        PerformSurgery(params);
    } catch (const TErrorException& ex) {
        YT_TLOG_ERROR("Changelog surgery failed")
            .With(ex);
    }
}

} // namespace NYT

int main(int argc, char** argv)
{
    NYT::Run(argc, argv);

    return 0;
}
