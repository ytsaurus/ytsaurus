#include "validate_snapshot.h"
#include "automaton.h"
#include "config.h"
#include "snapshot.h"
#include "hydra_context.h"

#include <yt/yt/client/hydra/version.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

void ValidateSnapshot(
    const IAutomatonPtr& automaton,
    const ISnapshotReaderPtr& reader,
    const TSnapshotValidationOptions& options)
{
    WaitFor(reader->Open())
        .ThrowOnError();

    automaton->SetSnapshotValidationOptions(options);

    YT_LOG_INFO("Snapshot validation started");
    automaton->Clear();

    try {
        automaton->LoadSnapshot(reader);

        // Mocking context and random seed to copy normal load behaviour.
        TVersion hydraContextVersion(0, -1);
        ui64 hydraContextRandomSeed = 0;
        HashCombine(hydraContextRandomSeed, 0);

        THydraContext hydraContext(
            hydraContextVersion,
            /*timestamp*/ {},
            hydraContextRandomSeed);
        THydraContextGuard hydraContextGuard(&hydraContext);

        automaton->PrepareState();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Snapshot loading failed");
        throw;
    } catch (const TFiberCanceledException&) {
        YT_LOG_INFO("Snapshot load fiber was canceled");
        throw;
    } catch (...) {
        YT_LOG_ERROR("Snapshot loading failed with an unknown error");
        throw;
    }

    YT_LOG_INFO("Finished loading snapshot");

    YT_LOG_INFO("Checking invariants");
    automaton->CheckInvariants();

    YT_LOG_INFO("Snapshot validation completed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
