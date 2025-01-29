#include <util/system/mutex.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>

namespace NYql {

class TFmrJobFactory: public IFmrJobFactory {
public:
    TFmrJobFactory(const TFmrJobFactorySettings& settings)
        : NumThreads_(settings.NumThreads), Function_(settings.Function)
    {
        Start();
    }

    ~TFmrJobFactory() {
        Stop();
    }

    NThreading::TFuture<ETaskStatus> StartJob(TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) override {
        auto promise = NThreading::NewPromise<ETaskStatus>();
        auto future = promise.GetFuture();
        auto startJobFunc = [&, task, cancelFlag, promise = std::move(promise)] () mutable {
            promise.SetValue(Function_(task, cancelFlag));
        };
        ThreadPool_->SafeAddFunc(startJobFunc);
        return future;
    }

private:
    void Start() {
        ThreadPool_ = CreateThreadPool(NumThreads_);
    }

    void Stop() {
        ThreadPool_->Stop();
    }

private:
    THolder<IThreadPool> ThreadPool_;
    i32 NumThreads_;
    std::function<ETaskStatus(TTask::TPtr, std::shared_ptr<std::atomic<bool>>)> Function_;
};

TFmrJobFactory::TPtr MakeFmrJobFactory(const TFmrJobFactorySettings& settings) {
    return MakeIntrusive<TFmrJobFactory>(settings);
}

} // namespace NYql
