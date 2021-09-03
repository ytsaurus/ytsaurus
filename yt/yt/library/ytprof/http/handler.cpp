#include "handler.h"

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

class TCpuProfilerHandler
    : public IHttpHandler
{
public:
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
            Symbolize(&profile);

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
    : public IHttpHandler
{
public:
    TTCMallocSnapshotProfilerHandler(tcmalloc::ProfileType profileType)
        : ProfileType_(profileType)
    { }

    void HandleRequest(const IRequestPtr& /* req */, const IResponseWriterPtr& rsp) override
    {
        try {
            auto profile = ReadHeapProfile(ProfileType_);

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

private:
    tcmalloc::ProfileType ProfileType_;
};

class TTCMallocAllocationProfilerHandler
    : public IHttpHandler
{
public:
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

void Register(const IServerPtr& server, const TString& prefix)
{
    server->AddHandler(prefix + "/cpu", New<TCpuProfilerHandler>());
    server->AddHandler(prefix + "/heap", New<TTCMallocSnapshotProfilerHandler>(tcmalloc::ProfileType::kHeap));
    server->AddHandler(prefix + "/peak", New<TTCMallocSnapshotProfilerHandler>(tcmalloc::ProfileType::kPeakHeap));
    server->AddHandler(prefix + "/fragmentation", New<TTCMallocSnapshotProfilerHandler>(tcmalloc::ProfileType::kFragmentation));
    server->AddHandler(prefix + "/allocations", New<TTCMallocAllocationProfilerHandler>());
    server->AddHandler(prefix + "/tcmalloc", New<TTCMallocStatHandler>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
