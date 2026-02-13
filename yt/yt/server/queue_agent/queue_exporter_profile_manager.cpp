#include "queue_exporter_profile_manager.h"

namespace NYT::NQueueAgent {

using namespace NProfiling;
using namespace NLogging;
using namespace NQueueClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TQueueExporterProfileManager
    : public NDetail::TProfileManagerBase<int>
{
public:
    explicit TQueueExporterProfileManager(
        const TProfiler& profiler,
        const std::string& exportName,
        const TLogger& logger,
        const TQueueTableRow& row,
        bool leading)
        : TProfileManagerBase(
            {
                {
                    EProfilerScope::Object,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Queue>(row))
                        .WithTag("export_name", exportName)
                        .WithGlobal()
                        .WithPrefix("/queue/static_export"),
                },
                {
                    EProfilerScope::ObjectPass,
                    profiler
                        .WithTags(NDetail::CreateObjectProfilingTags<EObjectKind::Queue>(row, /*enablePathAggregation*/ true, /*addObjectType*/ true, leading))
                        .WithTag("export_name", exportName)
                        .WithPrefix("/queue/static_export"),
                },
                {EProfilerScope::ObjectPartition, profiler},
                {EProfilerScope::AlertManager, profiler},
            })
        , Logger(logger)
    { }

    void Profile(
        const int& /*previousQueueSnapshot*/,
        const int& /*currentQueueSnapshot*/) override
    {
        // TODO(panesher): Change this to real snapshot and perform profile in profile manager.
        THROW_ERROR_EXCEPTION("TQueueExporterProfileManager::Profile unimplemented");
    }

private:
    const TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TQueueExporterProfileManager);

} // namespace

////////////////////////////////////////////////////////////////////////////////

IQueueExporterProfileManagerPtr CreateQueueExporterProfileManager(
    const NProfiling::TProfiler& profiler,
    const std::string& exportName,
    const NLogging::TLogger& logger,
    const NQueueClient::TQueueTableRow& row,
    bool leading)
{
    return New<TQueueExporterProfileManager>(profiler, exportName, logger, row, leading);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
