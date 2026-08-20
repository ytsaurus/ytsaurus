#include "file_resource.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TFileResourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("file_source", &TThis::FileSource);
}

////////////////////////////////////////////////////////////////////////////////

void TFileResourceDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("discover_period", &TThis::DiscoverPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(30));
    registrar.Parameter("update_retry_period", &TThis::UpdateRetryPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TFileResourceValidator::Validate(const TResourceSpec& spec)
{
    auto sourceNode = spec.Parameters->GetChildOrThrow("file_source");
    auto sourceSpec = ConvertTo<TFileSourceSpecPtr>(sourceNode);
    TRegistry::Get()->ValidateFileSourceSpec(sourceSpec);
}

////////////////////////////////////////////////////////////////////////////////

TMaterializedDirectory::TMaterializedDirectory(
    TFileSourceRevisionPtr revision,
    NFileStorage::IFileStorageObjectPtr storageObject)
    : Revision_(std::move(revision))
    , StorageObject_(std::move(storageObject))
    , RootPath_(StorageObject_->GetPath())
{ }

const TFileSourceRevisionPtr& TMaterializedDirectory::GetRevision() const
{
    return Revision_;
}

const std::string& TMaterializedDirectory::GetRootPath() const
{
    return RootPath_;
}

////////////////////////////////////////////////////////////////////////////////

void TFileResourceControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("file_source", &TThis::FileSource)
        .Default();
    registrar.Parameter("revision", &TThis::Revision)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TFileResourceController::TFileResourceController(
    TResourceControllerContextPtr context,
    TDynamicResourceControllerContextPtr dynamicContext)
    : TResourceControllerBase(std::move(context), std::move(dynamicContext))
    , Source_([&] {
        auto sourceContext = New<TFileSourceContext>();
        sourceContext->SourceSpec = GetParameters()->FileSource;
        sourceContext->ClientsCache = GetContext()->ClientsCache;
        sourceContext->PipelinePath = GetContext()->PipelinePath;
        sourceContext->Invoker = GetContext()->Invoker;
        sourceContext->Logger = GetContext()->Logger.WithTag("Component", "FileSource");
        return TRegistry::Get()->CreateFileSource(sourceContext);
    }())
    , DiscoveryError_(GetContext()->StatusProfiler->ErrorState("/discovery"))
    , DiscoveryExecutor_(New<TPeriodicExecutor>(
        GetContext()->Invoker,
        BIND(&TFileResourceController::Discover, MakeWeak(this)),
        GetDynamicParameters()->DiscoverPeriod))
{
    SubscribeReconfigured(BIND([
        this
    ] (const TDynamicResourceControllerContextPtr& /*dynamicContext*/) {
        DiscoveryExecutor_->SetPeriod(GetDynamicParameters()->DiscoverPeriod);
    }));
}

void TFileResourceController::Init(IInitContextPtr initContext)
{
    if (initContext) {
        initContext->InitClient<TFileResourceControllerState>(State_, "v0");

        auto fileSource = ConvertToNode(GetParameters()->FileSource)->AsMap();
        if (State_->Revision &&
            State_->FileSource &&
            AreNodesEqual(State_->FileSource, fileSource))
        {
            auto guard = Guard(Lock_);
            Revision_ = State_->Revision;
        } else {
            State_->FileSource = std::move(fileSource);
            State_->Revision.Reset();
        }
    }
    DiscoveryExecutor_->Start();
}

INodePtr TFileResourceController::BuildTargetRevisionSpec()
{
    auto guard = Guard(Lock_);
    return Revision_ ? ConvertToNode(Revision_) : nullptr;
}

void TFileResourceController::CollectStatuses(
    const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
    const TWorkerResourceStatusPtr& /*controllerStatus*/)
{
    THashMap<std::pair<i64, std::string>, i64> revisionCounts;
    THashMap<EFileResourceUpdateState, i64> updateStateCounts;
    for (const auto& [address, status] : workerStatuses) {
        Y_UNUSED(address);
        if (!status) {
            continue;
        }
        if (status->AppliedRevisionId) {
            ++revisionCounts[std::pair(*status->AppliedRevisionId, std::string("applied"))];
        }
        if (status->TargetRevisionId) {
            ++revisionCounts[std::pair(*status->TargetRevisionId, std::string("target"))];
        }
        if (status->UpdateState) {
            ++updateStateCounts[*status->UpdateState];
        }
    }

    auto guard = Guard(Lock_);
    for (auto it = RevisionGauges_.begin(); it != RevisionGauges_.end();) {
        if (!revisionCounts.contains(it->first)) {
            it->second.Update(0);
            auto toErase = it++;
            RevisionGauges_.erase(toErase);
        } else {
            ++it;
        }
    }
    for (const auto& [key, count] : revisionCounts) {
        auto [it, inserted] = RevisionGauges_.emplace(key, TGauge{});
        if (inserted) {
            it->second = GetContext()->Profiler.WithTag("revision_id", ToString(key.first)).WithTag("kind", key.second).Gauge("/revision_instance_count");
        }
        it->second.Update(count);
    }
    RevisionCounts_ = std::move(revisionCounts);
    UpdateStateCounts_ = std::move(updateStateCounts);
}

IMapNodePtr TFileResourceController::GetView()
{
    auto guard = Guard(Lock_);
    // clang-format off
    return BuildYsonNodeFluently()
        .BeginMap()
            .Item("source_revision").Value(Revision_)
            .Item("revision_instance_counts").DoMapFor(RevisionCounts_, [] (auto fluent, const auto& pair) {
                fluent.Item(Format("%v/%v", pair.first.first, pair.first.second)).Value(pair.second);
            })
            .Item("update_state_counts").DoMapFor(UpdateStateCounts_, [] (auto fluent, const auto& pair) {
                fluent.Item(FormatEnum(pair.first)).Value(pair.second);
            })
        .EndMap()
        ->AsMap();
    // clang-format on
}

void TFileResourceController::Discover()
{
    try {
        auto revision = WaitFor(Source_->Discover()).ValueOrThrow();
        if (revision) {
            {
                auto guard = Guard(Lock_);
                Revision_ = revision;
                if (State_.IsInitialized()) {
                    State_->Revision = std::move(revision);
                }
            }
            DiscoveryError_->ClearError();
            return;
        }

        bool hasPublishedRevision;
        {
            auto guard = Guard(Lock_);
            hasPublishedRevision = static_cast<bool>(Revision_);
        }
        if (hasPublishedRevision) {
            DiscoveryError_->ClearError();
        } else {
            DiscoveryError_->SetError(
                TError("File source discovery returned no revision"));
        }
    } catch (const std::exception& ex) {
        auto error = TError("File source discovery failed").With(ex);
        DiscoveryError_->SetError(error);
        YT_TLOG_WARNING("File source discovery failed")
            .With(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
