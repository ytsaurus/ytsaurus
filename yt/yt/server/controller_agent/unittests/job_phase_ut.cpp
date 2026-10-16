#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NExecNode;

template <class TSerializedPhase>
EJobPhase LoadJobPhase(TSerializedPhase serializedPhase, ESnapshotVersion version)
{
    TStringStream stream;
    NTableClient::TSaveContext saveContext(&stream, ToUnderlying(version));
    NYT::Save(saveContext, serializedPhase);
    saveContext.Finish();

    TLoadContext loadContext(&stream, New<NTableClient::TRowBuffer>(), version);
    EJobPhase phase = EJobPhase::Missing;
    TJobPhaseSerializer::Load(loadContext, phase);
    return phase;
}

TEST(TJobPhaseCompatibilityTest, ReadsLegacyPhase)
{
    NProto::TJobStatus status;
    status.set_phase_old(::NYT::ToProto(EJobPhaseOld::PreparingSlotDirectories));

    TJobSummary summary(&status);

    EXPECT_EQ(summary.Phase, EJobPhase::PreparingSlotDirectories);
}

TEST(TJobPhaseCompatibilityTest, PrefersNewPhase)
{
    NProto::TJobStatus status;
    status.set_phase_old(::NYT::ToProto(EJobPhaseOld::PreparingLayers));
    status.set_phase(::NYT::ToProto(EJobPhase::PreparingSlotDirectories));

    TJobSummary summary(&status);

    EXPECT_EQ(summary.Phase, EJobPhase::PreparingSlotDirectories);
}

TEST(TJobPhaseCompatibilityTest, KeepsMissingPhase)
{
    NProto::TJobStatus status;

    TJobSummary summary(&status);

    EXPECT_EQ(summary.Phase, EJobPhase::Missing);
}

TEST(TJobPhaseCompatibilityTest, LoadsLegacySnapshotPhase)
{
    auto version = ESnapshotVersion::DropLegacyDataSliceRepresentation;

    EXPECT_EQ(LoadJobPhase(EJobPhaseOld::PreparingLayers, version), EJobPhase::PreparingLayers);
    EXPECT_EQ(LoadJobPhase(EJobPhaseOld::PreparingSlotDirectories, version), EJobPhase::PreparingSlotDirectories);
}

TEST(TJobPhaseCompatibilityTest, LoadsCurrentSnapshotPhase)
{
    EXPECT_EQ(
        LoadJobPhase(EJobPhase::PreparingSlotDirectories, ESnapshotVersion::JobPhaseSpacing),
        EJobPhase::PreparingSlotDirectories);
}

TEST(TJobPhaseCompatibilityTest, SavesCurrentSnapshotPhase)
{
    TStringStream stream;
    TSaveContext saveContext(&stream);
    TJobPhaseSerializer::Save(saveContext, EJobPhase::PreparingSlotDirectories);
    saveContext.Finish();

    TStreamLoadContext loadContext(&stream);
    EXPECT_EQ(NYT::Load<i32>(loadContext), ToUnderlying(EJobPhase::PreparingSlotDirectories));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
