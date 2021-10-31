#include "handler.h"
#include "util/stream/fwd.h"

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/library/ytprof/cpu_profiler.h>
#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/profile.h>
#include <yt/yt/library/ytprof/symbolize.h>

#include <library/cpp/cgiparam/cgiparam.h>

namespace NYT::NYTProf {

using namespace NHttp;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TBaseHandler
    : public IHttpHandler
{
public:
    explicit TBaseHandler(const TBuildInfo& buildInfo)
        : BuildInfo_(buildInfo)
    { }

protected:
    const TBuildInfo BuildInfo_;
};

class TCpuProfilerHandler
    : public TBaseHandler
{
public:
    using TBaseHandler::TBaseHandler;

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        try {
            TCgiParameters params(req->GetUrl().RawQuery);

            auto duration = TDuration::Seconds(15);
            if (auto it = params.Find("d"); it != params.end()) {
                duration = TDuration::Parse(it->second);
            }

            TCpuProfiler profiler;
            profiler.Start();
            TDelayedExecutor::WaitForDuration(duration);
            profiler.Stop();

            auto profile = profiler.ReadProfile();
            Symbolize(&profile, true);
            AddBuildInfo(&profile, BuildInfo_);

            TStringStream profileBlob;
            WriteProfile(&profileBlob, profile);

            rsp->SetStatus(EStatusCode::OK);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(profileBlob.Str())))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            if (rsp->IsHeadersFlushed()) {
                throw;
            }

            rsp->SetStatus(EStatusCode::InternalServerError);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(ex.what())))
                .ThrowOnError();

            throw;
        }
    }
};

class TTCMallocSnapshotProfilerHandler
    : public TBaseHandler
{
public:
    TTCMallocSnapshotProfilerHandler(const TBuildInfo& buildInfo, tcmalloc::ProfileType profileType)
        : TBaseHandler(buildInfo)
        , ProfileType_(profileType)
    { }

    void HandleRequest(const IRequestPtr& /* req */, const IResponseWriterPtr& rsp) override
    {
        try {
            auto profile = ReadHeapProfile(ProfileType_);

            TStringStream profileBlob;
            WriteProfile(&profileBlob, profile);
            AddBuildInfo(&profile, BuildInfo_);

            rsp->SetStatus(EStatusCode::OK);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(profileBlob.Str())))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            if (rsp->IsHeadersFlushed()) {
                throw;
            }

            rsp->SetStatus(EStatusCode::InternalServerError);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(ex.what())))
                .ThrowOnError();

            throw;
        }
    }

private:
    tcmalloc::ProfileType ProfileType_;
};

class TTCMallocAllocationProfilerHandler
    : public TBaseHandler
{
public:
    using TBaseHandler::TBaseHandler;

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        try {
            TCgiParameters params(req->GetUrl().RawQuery);

            auto duration = TDuration::Seconds(15);
            if (auto it = params.Find("d"); it != params.end()) {
                duration = TDuration::Parse(it->second);
            }

            auto token = tcmalloc::MallocExtension::StartAllocationProfiling();

            TDelayedExecutor::WaitForDuration(duration);

            auto profile = ConvertAllocationProfile(std::move(token).Stop());

            TStringStream profileBlob;
            WriteProfile(&profileBlob, profile);
            AddBuildInfo(&profile, BuildInfo_);

            rsp->SetStatus(EStatusCode::OK);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(profileBlob.Str())))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            if (rsp->IsHeadersFlushed()) {
                throw;
            }

            rsp->SetStatus(EStatusCode::InternalServerError);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(ex.what())))
                .ThrowOnError();

            throw;
        }
    }
};

class TTCMallocStatHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /* req */, const IResponseWriterPtr& rsp) override
    {
        auto stat = tcmalloc::MallocExtension::GetStats();
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(TString{stat})))
            .ThrowOnError();
    }
};

class TBinaryHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        try {
            auto buildId = GetBuildId();
            TCgiParameters params(req->GetUrl().RawQuery);

            if (auto it = params.Find("check_build_id"); it != params.end()) {
                if (it->second != buildId) {
                    THROW_ERROR_EXCEPTION("Wrong build id: %v != %v", it->second, buildId);
                }
            }

            rsp->SetStatus(EStatusCode::OK);

            TFileInput file{"/proc/self/exe"};
            auto adapter = CreateBufferedSyncAdapter(rsp);
            file.ReadAll(*adapter);
            adapter->Finish();

            WaitFor(rsp->Close())
                .ThrowOnError();
        } catch (const std::exception& ex) {
            if (rsp->IsHeadersFlushed()) {
                throw;
            }

            rsp->SetStatus(EStatusCode::InternalServerError);
            WaitFor(rsp->WriteBody(TSharedRef::FromString(ex.what())))
                .ThrowOnError();

            throw;
        }
    }
};

class TVersionHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /* req */, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(GetVersion())))
            .ThrowOnError();
    }
};

class TBuildIdHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /* req */, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(GetVersion())))
            .ThrowOnError();
    }
};

void Register(
    const IServerPtr& server,
    const TString& prefix,
    const TBuildInfo& buildInfo)
{
    server->AddHandler(prefix + "/cpu", New<TCpuProfilerHandler>(buildInfo));
    server->AddHandler(prefix + "/heap", New<TTCMallocSnapshotProfilerHandler>(buildInfo, tcmalloc::ProfileType::kHeap));
    server->AddHandler(prefix + "/peak", New<TTCMallocSnapshotProfilerHandler>(buildInfo, tcmalloc::ProfileType::kPeakHeap));
    server->AddHandler(prefix + "/fragmentation", New<TTCMallocSnapshotProfilerHandler>(buildInfo, tcmalloc::ProfileType::kFragmentation));
    server->AddHandler(prefix + "/allocations", New<TTCMallocAllocationProfilerHandler>(buildInfo));

    server->AddHandler(prefix + "/tcmalloc", New<TTCMallocStatHandler>());

    server->AddHandler(prefix + "/binary", New<TBinaryHandler>());

    server->AddHandler(prefix + "/version", New<TVersionHandler>());
    server->AddHandler(prefix + "/buildid", New<TBuildIdHandler>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
