#include <yt/yt/core/rpc/unittests/lib/common.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NRpc {
namespace {

template <class TImpl>
using TRpcShutdownTest = TTestBase<TImpl>;

TYPED_TEST_SUITE(TRpcShutdownTest, TAllTransports);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TRpcShutdownTest, Shutdown)
{
    TMyProxy proxy(this->CreateChannel());

    std::vector<NYT::TFuture<typename TTypedClientResponse<NMyRpc::TRspSomeCall>::TResult>> futures;
    futures.reserve(100000);
    for (int i = 0; i < 100000; ++i) {
        auto req = proxy.SomeCall();
        req->SetTimeout(TDuration::Seconds(1));
        req->set_a(42);
        futures.push_back(req->Invoke());
    }
    NYT::Shutdown();
    for (auto& future : futures) {
        future.Cancel(TError{});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
