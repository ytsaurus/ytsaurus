#include "stdafx.h"
#include "evaluator.h"

#include "cg_cache.h"
#include "cg_fragment.h"
#include "cg_fragment_compiler.h"
#include "cg_routines.h"

#include "plan_fragment.h"
#include "plan_node.h"

#include "private.h"

#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <core/concurrency/fiber.h>

#include <llvm/Support/Threading.h>
#include <llvm/Support/TargetSelect.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;

static auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

void InitializeLlvmImpl()
{
    llvm::llvm_start_multithreaded();
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetAsmPrinter();
}

void InitializeLlvm()
{
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &InitializeLlvmImpl);
}


////////////////////////////////////////////////////////////////////////////////

class TEvaluator::TImpl
    : public NNonCopyable::TNonCopyable
{
public:
    TImpl()
    {
        InitializeLlvm();
        RegisterCGRoutines();

        Compiler_ = CreateFragmentCompiler();
    }

    TError Run(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemafulWriterPtr writer)
    {
        return EvaluateViaCache(
            callbacks,
            fragment,
            std::move(writer));
    }

private:
    TCGCache Cache_;
    TCGFragmentCompiler Compiler_;

    TError EvaluateViaCache(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemafulWriterPtr writer)
    {
        auto result = Cache_.Codegen(fragment, Compiler_);

        auto codegenedFunction = result.first;
        auto fragmentParams = result.second;

        // Make TRow from fragmentParams.ConstantArray.
        TChunkedMemoryPool memoryPool;
        auto constants = TRow::Allocate(&memoryPool, fragmentParams.ConstantArray.size());
        for (int i = 0; i < fragmentParams.ConstantArray.size(); ++i) {
            constants[i] = fragmentParams.ConstantArray[i];
        }

        try {
            LOG_DEBUG("Evaluating plan fragment (FragmentId: %s)",
                ~ToString(fragment.Id()));

            LOG_DEBUG("Opening writer (FragmentId: %s)",
                ~ToString(fragment.Id()));
            {
                auto error = WaitFor(writer->Open(
                    fragment.GetHead()->GetTableSchema(),
                    fragment.GetHead()->GetKeyColumns()));
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            LOG_DEBUG("Writer opened (FragmentId: %s)",
                ~ToString(fragment.Id()));

            TPassedFragmentParams passedFragmentParams;
            passedFragmentParams.Callbacks = callbacks;
            passedFragmentParams.Context = fragment.GetContext().Get();
            passedFragmentParams.DataSplitsArray = &fragmentParams.DataSplitsArray;

            std::vector<TRow> batch;
            batch.reserve(MaxRowsPerWrite);
            TRowBuffer pools;

            codegenedFunction(
                constants,
                &passedFragmentParams,
                &batch,
                &pools,
                writer.Get());

            LOG_DEBUG("Flushing writer (FragmentId: %s)",
                ~ToString(fragment.Id()));

            if (!batch.empty()) {
                if (!writer->Write(batch)) {
                    auto error = WaitFor(writer->GetReadyEvent());
                    THROW_ERROR_EXCEPTION_IF_FAILED(error);
                }
            }

            LOG_DEBUG("Closing writer (FragmentId: %s)",
                ~ToString(fragment.Id()));
            {
                auto error = WaitFor(writer->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            LOG_DEBUG("Finished evaluating plan fragment (FragmentId: %s)",
                ~ToString(fragment.Id()));
        } catch (const std::exception& ex) {
            auto error = TError("Failed to evaluate plan fragment") << ex;
            LOG_ERROR(error);
            return error;
        }

        return TError();
    }

};

////////////////////////////////////////////////////////////////////////////////

TEvaluator::TEvaluator()
    : Impl_(std::make_unique<TEvaluator::TImpl>())
{ }

TEvaluator::~TEvaluator()
{ }

TError TEvaluator::Run(
    IEvaluateCallbacks* callbacks,
    const TPlanFragment& fragment,
    ISchemafulWriterPtr writer)
{
    return Impl_->Run(callbacks, fragment, std::move(writer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
