
#include <yt/yt/core/actions/future.h>

namespace NYT::NTest {

template <typename TResult>
class TRetrier
{
public:
    TRetrier(std::function<TFuture<TResult>(int)> call)
        : Logger("retry")
        , Call_(call)
        , Attempt_(0)
        , Result_(NewPromise<TResult>())
    {
    }

    TFuture<TResult> Run()
    {
        Start();
        return Result_.ToFuture();
    }

    TRetrier(const TRetrier&) = delete;
    TRetrier(TRetrier&&) noexcept = delete;
    TRetrier& operator=(const TRetrier&) = delete;
    TRetrier& operator=(TRetrier&&) noexcept = delete;

private:
    NLogging::TLogger Logger;
    std::function<TFuture<TResult>(int)> Call_;
    int Attempt_;
    TPromise<TResult> Result_;

    bool IsRetriable(const TError& error)
    {
        TErrorCode code = error.GetCode();
        return
            code == TErrorCode(NRpc::EErrorCode::TransportError) ||
            code == NRpc::EErrorCode::TransientFailure ||
            code == NRpc::EErrorCode::RequestQueueSizeLimitExceeded ||
            code == EErrorCode::Timeout;
    }

    void MaybeRetry(TErrorOr<TResult>&& result)
    {
        if (result.IsOK()) {
            Result_.Set(result.ValueOrThrow());
            return;
        }
        if (!IsRetriable(result)) {
            Result_.Set(result);
            return;
        }
        YT_LOG_INFO("Retry %v", result);
        Start();
    }

    void Start()
    {
        YT_UNUSED_FUTURE(
            Call_(Attempt_++).ApplyUnique(BIND(&TRetrier::MaybeRetry, this)));
    }
};

}  // namespace NYT::NTest
