#include "skynet_api.h"

#include "private.h"

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/subprocess.h>

namespace NYT {
namespace NSkynetManager {

using namespace NConcurrency;

static auto& Logger = SkynetManagerLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr auto SkynetToolPath = "/skynet/tools/skybone-mds-ctl";

class TSkynetApi
    : public ISkynetApi
{
public:
    TSkynetApi(const IInvokerPtr invoker)
        : Invoker_(invoker)
    { }

    virtual TFuture<void> AddResource(
        const TString& rbTorrentId,
        const TString& discoveryUrl,
        const TString& rbTorrent) override
    {
        return BIND(&TSkynetApi::DoAddResource, MakeStrong(this), rbTorrentId, discoveryUrl, rbTorrent)
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual TFuture<void> RemoveResource(const TString& rbTorrentId) override
    {
        return BIND(&TSkynetApi::DoRemoveResource, MakeStrong(this), rbTorrentId)
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual TFuture<std::vector<TString>> ListResources() override
    {
        return BIND(&TSkynetApi::DoListResources, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    IInvokerPtr Invoker_;

    void DoAddResource(
        const TString& rbTorrentId,
        const TString& discoveryUrl,
        const TString& rbTorrent)
    {
        // Skynet tool expects rbTorrent and discoveryUrl passed to stdin in msgpack format.
        // And we really don't want to pull msgpack dependency just for this single use case.
        //
        // So instead we engage in string concatenation activity and
        // ask skynet to run convesion for us.

        auto conversionScript = Format(
            "import sys, msgpack;"
            "sys.stdout.write(msgpack.dumps({'uid': '%v', 'head': sys.stdin.read(), 'info': {'yt_lookup_uri': '%v'}}))",
            rbTorrentId,
            discoveryUrl);

        LOG_INFO("Running msgpack conversion (rbTorrentId: %v)", rbTorrentId);
        TSubprocess conversionProcess("/skynet/python/bin/python", false);
        conversionProcess.AddArguments({"-c", conversionScript});

        auto conversionResult = conversionProcess.Execute(TSharedRef::FromString(rbTorrent));
        auto msgpackedDescription = conversionResult.Output;
        if (!conversionResult.Status.IsOK()) {
            THROW_ERROR_EXCEPTION("Resource conversion failed")
                << TErrorAttribute("stderr", ToString(conversionResult.Error))
                << conversionResult.Status;
        }

        LOG_INFO("Adding resource (rbTorrentId: %v)", rbTorrentId);
        TSubprocess toolProcess(SkynetToolPath, false);
        toolProcess.AddArguments({"-f", "msgpack", "resource_add"});

//            TFileOutput dumpResource("resource.msgpack");
//            dumpResource.Write(ToString(msgpackedDescription));
//            dumpResource.Finish();

        auto toolResult = toolProcess.Execute(msgpackedDescription);
        if (!toolResult.Status.IsOK()) {
            THROW_ERROR_EXCEPTION("Resource addition failed")
                << TErrorAttribute("stderr", ToString(toolResult.Error))
                << toolResult.Status;
        }

        LOG_INFO("Resource added (rbTorrentId: %v)", rbTorrentId);
    }

    void DoRemoveResource(const TString& rbTorrentId)
    {
        LOG_INFO("Removing resource (rbTorrentId: %v)", rbTorrentId);

        TSubprocess toolProcess(SkynetToolPath, false);
        toolProcess.AddArguments({"resource_remove", rbTorrentId});

        auto toolResult = toolProcess.Execute();
        if (!toolResult.Status.IsOK()) {
            THROW_ERROR_EXCEPTION("Resource deletion failed")
                << TErrorAttribute("stderr", ToString(toolResult.Error))
                << toolResult.Status;
        }

        LOG_INFO("Resource removed (rbTorrentId: %v)", rbTorrentId);
    }

    std::vector<TString> DoListResources()
    {
        LOG_INFO("Listing resources");

        TSubprocess toolProcess(SkynetToolPath, false);
        toolProcess.AddArgument("resource_list");

        auto toolResult = toolProcess.Execute();
        if (!toolResult.Status.IsOK()) {
            THROW_ERROR_EXCEPTION("Resource listing failed")
                << TErrorAttribute("stderr", ToString(toolResult.Error))
                << toolResult.Status;
        }

        auto output = ToString(toolResult.Output);
        TStringInput input(output);
        TString line;
        std::vector<TString> resources;
        while (input.ReadTo(line, '\n')) {
            resources.push_back(line);
        }

        LOG_INFO("Listed %d resources", resources.size());
        return resources;
    }
};

DEFINE_REFCOUNTED_TYPE(TSkynetApi)
DECLARE_REFCOUNTED_TYPE(TSkynetApi)

ISkynetApiPtr CreateShellSkynetApi(const IInvokerPtr& invoker)
{
    return New<TSkynetApi>(invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
