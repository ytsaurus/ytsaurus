#include "execution_block.h"

#include <yt/cpp/roren/interface/private/par_do_tree.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NRoren::NPrivate {

using NYT::TFuture;
using NYT::IInvokerPtr;

////////////////////////////////////////////////////////////////////////////////

static TRowVtable GetRowVtable(const IRawParDoPtr parDo)
{
    auto tags = parDo->GetInputTags();
    Y_VERIFY(tags.size() == 1);
    return tags[0].GetRowVtable();
}

// indent is a parent indent
static void WriteParDoArg(IOutputStream* out, const IRawParDo& parDo, const TString& indent)
{
    (*out) << indent << "  ParDo: ";
    if (const auto* parDoTree = dynamic_cast<const IParDoTree*>(&parDo)) {
        (*out) << parDoTree->GetDebugDescription();
    } else {
        (*out) << NRoren::NPrivate::GetAttributeOrDefault(parDo, NameTag, TString{"<unknown>"});
    }
    (*out) << '\n';
}

static void WriteOutputsArg(IOutputStream* out, const std::vector<IExecutionBlockPtr>& outputs, const TString& indent)
{
    if (outputs.empty()) {
        return;
    }
    (*out) << indent << "  Outputs:\n";
    auto blockIndent = indent + TString(4, ' ');
    for (const auto& block : outputs) {
        block->WriteDebugDescription(out, blockIndent);
    }
}

using TErrorPropagator = NYT::TCallback<void(const NYT::TError&)>;
static TErrorPropagator MakeErrorPropagator(NYT::TPromise<void> promise)
{
    return BIND([promise = std::move(promise)] (const NYT::TError& error) {
        if (!error.IsOK()) {
            promise.TrySet(error);
        }
    });
}

static void CheckError(const NYT::TPromise<void>& promise)
{
    if (auto optionalError = promise.TryGet()) {
        optionalError->ThrowOnError();
        YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TString IExecutionBlock::GetDebugDescription() const
{
    TStringStream out;
    WriteDebugDescription(&out, 0);
    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

class TBlockPipe
    : public IRawOutput
{
public:
    explicit TBlockPipe(IExecutionBlockPtr output)
        : Output_(std::move(output))
        , Config_(Output_->GetInputPipeConfiguration())
        , RowVtable_(Output_->GetInputRowVtable())
        , BufferSize_(Config_.DesiredInputElementCount * RowVtable_.DataSize)
    {
        ResetBuffer();
    }

    void AddRaw(const void* rows, ssize_t count) override
    {
        const char* curRow = static_cast<const char*>(rows);
        for (ssize_t i = 0; i < count; ++i) {
            RowVtable_.CopyConstructor(BufferWriteOffset_, curRow);
            curRow += RowVtable_.DataSize;
            BufferWriteOffset_ += RowVtable_.DataSize;
            if (BufferWriteOffset_ == Buffer_.End()) {
                Flush();
            }
        }
    }

    void Close() override
    {
        if (BufferWriteOffset_ != Buffer_.begin()) {
            Flush();
        }
    }

private:
    void ResetBuffer()
    {
        Buffer_ = NYT::TSharedMutableRef::Allocate(BufferSize_);
        BufferWriteOffset_ = Buffer_.Begin();
    }

    void Flush()
    {
        Output_->AsyncDo(std::move(Buffer_.Slice(Buffer_.Begin(), BufferWriteOffset_)));
        ResetBuffer();
    }

private:
    const IExecutionBlockPtr Output_;
    const TBlockPipeConfig Config_;
    const TRowVtable RowVtable_;
    const ssize_t BufferSize_;

    NYT::TSharedMutableRef Buffer_;
    char* BufferWriteOffset_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<IRawOutputPtr> MakeBlockPipes(std::vector<IExecutionBlockPtr> outputBlocks)
{
    std::vector<IRawOutputPtr> result;

    for (auto&& block : outputBlocks) {
        result.push_back(MakeIntrusive<TBlockPipe>(block));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TExecutionBlockBase
    : public IExecutionBlock
{
public:
    TExecutionBlockBase(std::vector<IExecutionBlockPtr> outputBlocks)
        : OutputBlocks_(std::move(outputBlocks))
    { }

    void BindToInvoker(IInvokerPtr invoker)
    {
        YT_VERIFY(invoker != nullptr);
        YT_VERIFY(Invoker_ == nullptr);

        Invoker_ = std::move(invoker);
        for (const auto& output : OutputBlocks_) {
            output->BindToInvoker(Invoker_);
        }
    }

protected:
    const std::vector<IExecutionBlockPtr> OutputBlocks_;
    IInvokerPtr Invoker_;
};

////////////////////////////////////////////////////////////////////////////////

class TOutputExecutionBlock
    : public IExecutionBlock
{
public:
    TOutputExecutionBlock(TBlockPipeConfig config, IRawOutputPtr output, TRowVtable rowVtable)
        : Config_(std::move(config))
        , Output_(output)
        , RowVtable_(std::move(rowVtable))
    { }

    IExecutionBlockPtr Clone() override
    {
        return ::MakeIntrusive<TOutputExecutionBlock>(Config_, Output_, RowVtable_);
    }

    void BindToInvoker(IInvokerPtr invoker) override
    {
        Invoker_ = NYT::NConcurrency::CreateSerializedInvoker(invoker);
    }

    TFuture<void> Start(IExecutionContextPtr /*executionContext*/) override
    {
        Finished_ = NYT::NewPromise<void>();
        ErrorPropagator_ = MakeErrorPropagator(Finished_);
        return Finished_;
    }

    void AsyncDo(NYT::TSharedRef data) override
    {
        auto count = data.size() / RowVtable_.DataSize;
        BIND([output = Output_, count = count, data = data] {
            output->AddRaw(data.Begin(), count);
        })
            .AsyncVia(Invoker_)
            .Run()
            .Subscribe(ErrorPropagator_);
    }

    void AsyncFinish() override
    {
        auto finished = BIND([output = Output_] {
            output->Close();
        })
            .AsyncVia(Invoker_)
            .Run();

        Finished_.TrySetFrom(finished);
    }

    TBlockPipeConfig GetInputPipeConfiguration() const override
    {
        return Config_;
    }

    TRowVtable GetInputRowVtable() const override
    {
        return RowVtable_;
    }

    void WriteDebugDescription(IOutputStream*, const TString&) const override
    {
        Y_FAIL("not implemented");
    }

private:
    const TBlockPipeConfig Config_;
    const IRawOutputPtr Output_;
    const TRowVtable RowVtable_;

    IInvokerPtr Invoker_;

    NYT::TPromise<void> Finished_;
    TErrorPropagator ErrorPropagator_;
};

IExecutionBlockPtr CreateOutputExecutionBlock(
    TBlockPipeConfig config,
    IRawOutputPtr output,
    TRowVtable rowVtable)
{
    return ::MakeIntrusive<TOutputExecutionBlock>(std::move(config), std::move(output), std::move(rowVtable));
}

////////////////////////////////////////////////////////////////////////////////

class TSerializedExecutionBlock
    : public TExecutionBlockBase
{
public:
    TSerializedExecutionBlock(TBlockPipeConfig config, IRawParDoPtr parDo, std::vector<IExecutionBlockPtr> outputBlocks)
        : TExecutionBlockBase(std::move(outputBlocks))
        , InputPipeConfig_(config)
        , ParDo_(std::move(parDo))
        , InputRowVtable_(GetRowVtable(ParDo_))
        , Outputs_(MakeBlockPipes(OutputBlocks_))
    { }

    TSerializedExecutionBlock(TBlockPipeConfig config, IRawParDoPtr parDo, std::vector<IRawOutputPtr> outputs)
        : TExecutionBlockBase({})
        , InputPipeConfig_(config)
        , ParDo_(std::move(parDo))
        , InputRowVtable_(GetRowVtable(ParDo_))
        , Outputs_(outputs)
    { }

    IExecutionBlockPtr Clone() override
    {
        // We can clone only unbound execution blocks;
        Y_VERIFY(Invoker_ == nullptr);

        if (OutputBlocks_.size() > 0 || Outputs_.size() == 0) {
            std::vector<IExecutionBlockPtr> clonedOutputBlocks;
            clonedOutputBlocks.reserve(OutputBlocks_.size());
            for (const auto& block : OutputBlocks_) {
                clonedOutputBlocks.push_back(block->Clone());
            }
            return ::MakeIntrusive<TSerializedExecutionBlock>(
                InputPipeConfig_,
                ParDo_->Clone(),
                clonedOutputBlocks
            );
        } else {
            return ::MakeIntrusive<TSerializedExecutionBlock>(
                InputPipeConfig_,
                ParDo_->Clone(),
                Outputs_
            );
        }
    }

    void BindToInvoker(IInvokerPtr invoker) override
    {
        auto serializedInvoker = NYT::NConcurrency::CreateSerializedInvoker(invoker);
        TExecutionBlockBase::BindToInvoker(std::move(serializedInvoker));
    }

    TFuture<void> Start(IExecutionContextPtr executionContext) override
    {
        std::vector<TFuture<void>> childrenFutureList;
        childrenFutureList.reserve(OutputBlocks_.size());
        for (const auto& block : OutputBlocks_) {
            auto current = block->Start(executionContext);
            childrenFutureList.push_back(std::move(current));
        }

        ParDo_->Start(executionContext, std::move(Outputs_));

        Finished_ = NYT::NewPromise<void>();
        ErrorPropagator_ = MakeErrorPropagator(Finished_);
        ChildrenFinished_ = NYT::AllSucceeded(std::move(childrenFutureList));
        ChildrenFinished_.Subscribe(ErrorPropagator_);

        return Finished_;
    }

    void AsyncDo(NYT::TSharedRef data) override
    {
        CheckError(Finished_);

        auto count = data.Size() / InputRowVtable_.DataSize;
        BIND([parDo = ParDo_, data = std::move(data), count = count] {
            parDo->Do(data.Begin(), count);
        })
            .AsyncVia(Invoker_)
            .Run()
            .Subscribe(ErrorPropagator_);
    }

    void AsyncFinish() override
    {
        CheckError(Finished_);

        auto finished = BIND([parDo = ParDo_, childrenFinished = ChildrenFinished_, outputs=Outputs_, outputBlocks=OutputBlocks_] {
            parDo->Finish();
            for (const auto& output : outputs) {
                output->Close();
            }

            for (const auto& block : outputBlocks) {
                block->AsyncFinish();
            }

            NYT::NConcurrency::WaitFor(childrenFinished)
                .ThrowOnError();
        })
            .AsyncVia(Invoker_)
            .Run();

        Finished_.TrySetFrom(finished);
    }

    TBlockPipeConfig GetInputPipeConfiguration() const override
    {
        return InputPipeConfig_;
    }

    TRowVtable GetInputRowVtable() const override
    {
        return InputRowVtable_;
    }

    void WriteDebugDescription(IOutputStream* out, const TString& indent) const override
    {
        (*out) << indent << "TSerializedExecutionBlock\n";
        WriteParDoArg(out, *ParDo_, indent);
        WriteOutputsArg(out, OutputBlocks_, indent);
    }

private:
    const TBlockPipeConfig InputPipeConfig_;
    const IRawParDoPtr ParDo_;
    const TRowVtable InputRowVtable_;
    const std::vector<IRawOutputPtr> Outputs_;

    NYT::TPromise<void> Finished_;
    NYT::TFuture<void> ChildrenFinished_;
    TErrorPropagator ErrorPropagator_;
};

////////////////////////////////////////////////////////////////////////////////

IExecutionBlockPtr CreateSerializedExecutionBlock(
    TBlockPipeConfig config,
    IRawParDoPtr parDo,
    std::vector<IExecutionBlockPtr> outputs)
{
    return ::MakeIntrusive<TSerializedExecutionBlock>(config, parDo, outputs);
}

IExecutionBlockPtr CreateSerializedExecutionBlock(
    TBlockPipeConfig config,
    IRawParDoPtr parDo,
    std::vector<IRawOutputPtr> outputs)
{
    return ::MakeIntrusive<TSerializedExecutionBlock>(config, parDo, outputs);
}

////////////////////////////////////////////////////////////////////////////////

class TEpochAwareParDoPool
    : public TThrRefBase
{
public:
    class TAcquired;
    class TParDoClosure;
    using TParDoClosurePtr = ::TIntrusivePtr<TParDoClosure>;

public:
    TEpochAwareParDoPool(IRawParDoPtr parDo, std::vector<IExecutionBlockPtr> executionBlocks)
        : Prototype_(
            ::MakeIntrusive<TParDoClosure>(
                parDo,
                std::make_shared<std::vector<IExecutionBlockPtr>>(std::move(executionBlocks)
            )))
    { }

    TAcquired Acquire()
    {
        auto g = Guard(SpinLock_);
        ++InFlight_;
        if (!ThisEpochItems_.empty()) {
            auto item = std::move(ThisEpochItems_.back());
            ThisEpochItems_.pop_back();
            return {std::move(item), false, *this};
        } else {
            if (PreviousEpochItems_.empty()) {
                PreviousEpochItems_.push_back(Prototype_->Clone());
            }

            auto item = std::move(PreviousEpochItems_.back());
            PreviousEpochItems_.pop_back();
            return {std::move(item), true, *this};
        }
    }

    TFuture<std::vector<TParDoClosurePtr>> FinishEpoch()
    {
        NYT::TFuture<void> allReleased;

        {
            auto g = Guard(SpinLock_);
            Finished_ = true;
            if (InFlight_ == 0) {
                allReleased = NYT::MakeFuture<void>(NYT::TError{});
            } else {
                AllReleased_ = NYT::NewPromise<void>();
                allReleased = AllReleased_;
            }
        }

        // NB. This application happens either directly in this function or in DoRelease,
        // so it's safe to capture `this`.
        return allReleased.Apply(BIND([this] {
            std::vector<TParDoClosurePtr> result = std::move(ThisEpochItems_);
            PreviousEpochItems_.insert(PreviousEpochItems_.end(), result.begin(), result.end());
            ThisEpochItems_.reserve(PreviousEpochItems_.size());
            Finished_ = false;
            return result;
        }));
    }

    const IRawParDoPtr& GetPrototype() const
    {
        return Prototype_->ParDo_;
    }


private:
    void DoRelease(TParDoClosurePtr item)
    {
        bool setAllReleasedPromise = false;
        {
            auto g = Guard(SpinLock_);
            --InFlight_;
            ThisEpochItems_.push_back(std::move(item));
            if (Finished_ && InFlight_ == 0) {
                setAllReleasedPromise = true;
            }
        }

        if (setAllReleasedPromise) {
            AllReleased_.Set();
        }
    }

private:
    YT_DECLARE_SPIN_LOCK(NYT::NThreading::TSpinLock, SpinLock_);
    TParDoClosurePtr Prototype_;
    std::vector<TParDoClosurePtr> ThisEpochItems_;
    std::vector<TParDoClosurePtr> PreviousEpochItems_;
    int InFlight_ = 0;
    bool Finished_ = false;
    NYT::TPromise<void> AllReleased_;

public:
    class TAcquired
    {
    public:
        TParDoClosurePtr ParDoClosure;
        bool EpochFirstAcquire;

    public:
        ~TAcquired()
        {
            Pool_.DoRelease(std::move(ParDoClosure));
        }

    private:
        TAcquired(TParDoClosurePtr item, bool epochFirstAcquire, TEpochAwareParDoPool& pool)
            : ParDoClosure(std::move(item))
            , EpochFirstAcquire(epochFirstAcquire)
            , Pool_(pool)
        { }

    private:
        TEpochAwareParDoPool& Pool_;

        friend class TEpochAwareParDoPool;
    };

    class TParDoClosure
        : public IClonable<TParDoClosure>
    {
        using TExecutionBlockListPtr = std::shared_ptr<std::vector<IExecutionBlockPtr>>;

    public:
        TParDoClosure(IRawParDoPtr parDo, TExecutionBlockListPtr outputBlocks)
            : ParDo_(std::move(parDo))
            , OutputBlocks_(std::move(outputBlocks))
            , Outputs_(MakeOutputs(*OutputBlocks_))
        { }

        void Start(const IExecutionContextPtr& executionContext)
        {
            ParDo_->Start(executionContext, Outputs_);
        }

        void Do(const void* data, size_t count)
        {
            ParDo_->Do(data, count);
        }

        void Finish()
        {
            ParDo_->Finish();
            for (auto&& output : Outputs_) {
                output->Close();
            }
        }

        TParDoClosurePtr Clone() const
        {
            return ::MakeIntrusive<TParDoClosure>(ParDo_->Clone(), OutputBlocks_);
        }

    private:
        std::vector<IRawOutputPtr> MakeOutputs(const std::vector<IExecutionBlockPtr>& blockList)
        {
            std::vector<IRawOutputPtr> result;
            result.reserve(blockList.size());
            for (const auto& block : blockList) {
                result.push_back(::MakeIntrusive<TBlockPipe>(block));
            }
            return result;
        }

    private:
        const IRawParDoPtr ParDo_;
        const TExecutionBlockListPtr OutputBlocks_;
        const std::vector<IRawOutputPtr> Outputs_;

        friend class TEpochAwareParDoPool;
    };
};

using TEpochAwareParDoPoolPtr = TIntrusivePtr<TEpochAwareParDoPool>;

////////////////////////////////////////////////////////////////////////////////

class TConcurrentExecutionBlock : public TExecutionBlockBase
{
public:
    TConcurrentExecutionBlock(TBlockPipeConfig config, IRawParDoPtr parDo, std::vector<IExecutionBlockPtr> outputBlocks)
        : TExecutionBlockBase(std::move(outputBlocks))
        , InputPipeConfig_(config)
        , InputRowVtable_(GetRowVtable(parDo))
        , ParDoPool_(MakeIntrusive<TEpochAwareParDoPool>(std::move(parDo), OutputBlocks_))
    { }

    IExecutionBlockPtr Clone() override
    {
        // We can clone only unbound execution blocks;
        Y_VERIFY(Invoker_ == nullptr);

        std::vector<IExecutionBlockPtr> clonedOutputBlocks;
        clonedOutputBlocks.reserve(OutputBlocks_.size());
        for (const auto& block : OutputBlocks_) {
            clonedOutputBlocks.push_back(block->Clone());
        }
        return ::MakeIntrusive<TConcurrentExecutionBlock>(
            InputPipeConfig_,
            ParDoPool_->GetPrototype(),
            clonedOutputBlocks
        );
    }

    void BindToInvoker(IInvokerPtr invoker) override
    {
        YT_VERIFY(Invoker_ == nullptr);
        YT_VERIFY(invoker != nullptr);
        Invoker_ = invoker;
        for (const auto& output : OutputBlocks_) {
            output->BindToInvoker(Invoker_);
        }
    }

    NYT::TFuture<void> Start(IExecutionContextPtr executionContext) override
    {
        std::vector<NYT::TFuture<void>> childrenFutureList;
        childrenFutureList.reserve(OutputBlocks_.size());
        for (const auto& block : OutputBlocks_) {
            auto future = block->Start(executionContext);
            childrenFutureList.push_back(std::move(future));
        }
        ChildrenFinished_ = NYT::AllSucceeded(std::move(childrenFutureList));

        ExecutionContext_ = std::move(executionContext);

        Finished_ = NYT::NewPromise<void>();
        ErrorPropagator_ = MakeErrorPropagator(Finished_);

        ChildrenFinished_.Subscribe(ErrorPropagator_);
        return Finished_;
    }

    void AsyncDo(NYT::TSharedRef data) override
    {
        CheckError(Finished_);

        auto count = data.Size() / InputRowVtable_.DataSize;
        BIND([
            parDoPool = ParDoPool_,
            executionContext = ExecutionContext_,
            data = std::move(data),
            count=count
        ] {
            auto acquired = parDoPool->Acquire();
            if (acquired.EpochFirstAcquire) {
                acquired.ParDoClosure->Start(executionContext);
            }
            acquired.ParDoClosure->Do(data.Begin(), count);
        })
            .AsyncVia(Invoker_)
            .Run()
            .Subscribe(ErrorPropagator_);
    }

    void AsyncFinish() override
    {
        auto finished = BIND([
            parDoPool = ParDoPool_,
            invoker = Invoker_,
            outputBlocks = OutputBlocks_
        ] {
            auto parDoClosureList = NYT::NConcurrency::WaitFor(parDoPool->FinishEpoch())
                .ValueOrThrow();

            std::vector<TFuture<void>> futures;
            futures.reserve(parDoClosureList.size());
            for (auto&& closure : parDoClosureList) {
                auto finished = BIND([closure = std::move(closure)] {
                    closure->Finish();
                })
                    .AsyncVia(invoker)
                    .Run();
                futures.push_back(std::move(finished));
            }

            NYT::NConcurrency::WaitFor(NYT::AllSucceeded(futures))
                .ThrowOnError();

            for (const auto& block : outputBlocks) {
                block->AsyncFinish();
            }
        })
            .AsyncVia(Invoker_)
            .Run();

        Finished_.TrySetFrom(NYT::AllSucceeded<void>({finished, ChildrenFinished_}));
    }

    TBlockPipeConfig GetInputPipeConfiguration() const override
    {
        return InputPipeConfig_;
    }

    TRowVtable GetInputRowVtable() const override
    {
        return InputRowVtable_;
    }

    void WriteDebugDescription(IOutputStream* out, const TString& indent) const override
    {
        (*out) << indent << "TConcurrentExecutionBlock\n";
        WriteParDoArg(out, *ParDoPool_->GetPrototype(), indent);
        WriteOutputsArg(out, OutputBlocks_, indent);
    }

private:
    const TBlockPipeConfig InputPipeConfig_;
    const TRowVtable InputRowVtable_;

    TEpochAwareParDoPoolPtr ParDoPool_;

    IExecutionContextPtr ExecutionContext_;

    NYT::TPromise<void> Finished_;
    NYT::TFuture<void> ChildrenFinished_;
    TErrorPropagator ErrorPropagator_;
};

IExecutionBlockPtr CreateConcurrentExecutionBlock(
        TBlockPipeConfig config,
        IRawParDoPtr parDo,
        std::vector<IExecutionBlockPtr> outputs)
{
    return ::MakeIntrusive<TConcurrentExecutionBlock>(
        std::move(config),
        std::move(parDo),
        std::move(outputs)
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
