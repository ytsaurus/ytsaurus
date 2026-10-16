#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/datetime/base.h>
#include <util/system/event.h>

#include <array>
#include <atomic>
#include <memory>

namespace NYql::NUdf {
    namespace {

        constexpr size_t FunctionCount = 2;

        struct TProbeState {
            std::array<std::atomic<bool>, FunctionCount> BuildSeen = {};
            std::atomic<ui32> ActiveBuilds = 0;
            std::atomic<ui32> BuildArrivals = 0;
            std::atomic<bool> BuildsOverlapped = false;
            TManualEvent BothBuildsEntered;

            std::atomic<ui32> ActiveExecutions = 0;
            std::atomic<ui32> ExecutionArrivals = 0;
            std::atomic<bool> ExecutionsOverlapped = false;
            TManualEvent BothExecutionsEntered;
        };

        class TProbeFunction final
            : public TBoxedValue {
        public:
            TProbeFunction(std::shared_ptr<TProbeState> state, ui32 result)
                : State_(std::move(state))
                , Result_(result)
            {
            }

        private:
            TUnboxedValue Run(
                const IValueBuilder*,
                const TUnboxedValuePod*) const final {
                if (State_->ActiveExecutions.fetch_add(1) > 0) {
                    State_->ExecutionsOverlapped.store(true);
                }

                if (State_->ExecutionArrivals.fetch_add(1) + 1 == FunctionCount) {
                    State_->BothExecutionsEntered.Signal();
                }

                const bool rendezvousSucceeded = State_->BothExecutionsEntered.WaitT(TDuration::Seconds(10));
                State_->ActiveExecutions.fetch_sub(1);

                const bool succeeded = rendezvousSucceeded &&
                                       State_->BuildArrivals.load() == FunctionCount &&
                                       State_->BuildsOverlapped.load() &&
                                       State_->ExecutionsOverlapped.load();
                return TUnboxedValuePod(succeeded ? Result_ : 0);
            }

        private:
            const std::shared_ptr<TProbeState> State_;
            const ui32 Result_;
        };

        class TConcurrentPatternBuildModule final
            : public IUdfModule {
        public:
            TStringRef Name() const {
                return TStringRef::Of("ConcurrentPatternBuild");
            }

            void CleanupOnTerminate() const final {
            }

            void GetAllFunctions(IFunctionsSink& sink) const final {
                sink.Add(TStringRef::Of("First"));
                sink.Add(TStringRef::Of("Second"));
            }

            void BuildFunctionTypeInfo(
                const TStringRef& name,
                TType*,
                const TStringRef&,
                ui32 flags,
                IFunctionTypeInfoBuilder& builder) const final {
                const size_t index = name == TStringRef::Of("First") ? 0 : 1;
                const ui32 result = index + 1;
                builder.SimpleSignature<ui32()>();

                if (flags & TFlags::TypesOnly) {
                    return;
                }

                // Pattern construction is the first full type-info build for each of
                // these functions. Later calls belong to the concurrently executed
                // graphs and must not participate in the build rendezvous.
                if (!State_->BuildSeen[index].exchange(true)) {
                    if (State_->ActiveBuilds.fetch_add(1) > 0) {
                        State_->BuildsOverlapped.store(true);
                    }

                    if (State_->BuildArrivals.fetch_add(1) + 1 == FunctionCount) {
                        State_->BothBuildsEntered.Signal();
                    }

                    State_->BothBuildsEntered.WaitT(TDuration::Seconds(10));
                    State_->ActiveBuilds.fetch_sub(1);
                }

                builder.Implementation(new TProbeFunction(State_, result));
            }

        private:
            const std::shared_ptr<TProbeState> State_ = std::make_shared<TProbeState>();
        };

    } // namespace
} // namespace NYql::NUdf

REGISTER_MODULES(NYql::NUdf::TConcurrentPatternBuildModule)
