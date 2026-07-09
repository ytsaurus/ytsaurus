#include "message_filter.h"

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/engine_api/query_evaluator.h>

#include <yt/yt/core/concurrency/context_switch.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/containers/insert_only_concurrent_cache/cache.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NQueryClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int MetaColumnCount = 5;

//! Compiled predicate over a single expression, with a per-payload-schema cache of
//! evaluation contexts (context creation does codegen and is expensive).
class TCompiledFilter
    : public TRefCounted
{
public:
    explicit TCompiledFilter(std::string expression)
        : Expression_(std::move(expression))
        , ParsedSource_(ParseSource(Expression_, EParseMode::Expression))
    { }

    const std::string& GetExpression() const
    {
        return Expression_;
    }

    bool ShouldSkip(const TMessage& message) const
    {
        const auto& context = GetEvaluationContext(message.PayloadSchema);

        const auto& payloadRow = message.Payload.Underlying();
        int payloadColumnCount = message.PayloadSchema->GetColumnCount();

        // Inline storage covers payload + meta columns for typical schemas, so there is
        // no per-message heap allocation.
        TCompactVector<TUnversionedValue, 32> inputValues;
        inputValues.reserve(payloadColumnCount + MetaColumnCount);
        for (int index = 0; index < payloadColumnCount; ++index) {
            inputValues.push_back(payloadRow[index]);
        }
        inputValues.push_back(MakeUnversionedStringValue(message.MessageId.Underlying()));
        inputValues.push_back(MakeUnversionedStringValue(message.StreamId.Underlying()));
        inputValues.push_back(MakeUnversionedUint64Value(message.SystemTimestamp.Underlying()));
        inputValues.push_back(MakeUnversionedUint64Value(message.EventTimestamp.Underlying()));
        inputValues.push_back(MakeUnversionedUint64Value(message.AlignmentTimestamp.Underlying()));

        // EvaluateQuery does not switch fiber context, so the thread-local buffer is safe.
        TForbidContextSwitchGuard contextSwitchGuard;
        static thread_local TRowBufferPtr BufferCache = New<TRowBuffer>();
        auto finally = Finally([&] {
            BufferCache->Clear();
        });

        auto result = EvaluateQuery(
            *context,
            TRange(inputValues.data(), inputValues.size()),
            BufferCache);

        // The predicate must evaluate to a boolean; anything else (including NULL) is a misconfigured expression.
        if (result.Type != EValueType::Boolean) {
            THROW_ERROR_EXCEPTION("Message filter expression must evaluate to a boolean value, but got %Qlv",
                    result.Type)
                << TErrorAttribute("expression", Expression_);
        }
        return result.Data.Boolean;
    }

private:
    const std::string Expression_;
    const std::unique_ptr<TParsedSource> ParsedSource_;

    struct TCacheEntry
    {
        TQueryEvaluationContextPtr Context;
        // Keeps the payload schema (cache key) alive.
        TTableSchemaPtr PayloadSchema;
    };

    mutable TInsertOnlyConcurrentCache<const TTableSchema*, TCacheEntry> ContextCache_;

    const TQueryEvaluationContextPtr& GetEvaluationContext(const TTableSchemaPtr& payloadSchema) const
    {
        const auto& entry = ContextCache_.FindOrInsert(payloadSchema.Get(), [&] {
            return TCacheEntry{
                .Context = CreateQueryEvaluationContext(*ParsedSource_, BuildEvaluationSchema(payloadSchema)),
                .PayloadSchema = payloadSchema,
            };
        });
        return entry.Context;
    }

    static TTableSchemaPtr BuildEvaluationSchema(const TTableSchemaPtr& payloadSchema)
    {
        std::vector<TColumnSchema> columns;
        columns.reserve(payloadSchema->GetColumnCount() + MetaColumnCount);
        // Payload columns are taken as-is, but stripped of sort order and computed-column
        // expressions so the predicate only reads from them.
        for (const auto& column : payloadSchema->Columns()) {
            columns.emplace_back(column.Name(), column.LogicalType());
        }
        columns.emplace_back(std::string(MessageIdFilterColumnName), EValueType::String);
        columns.emplace_back(std::string(StreamIdFilterColumnName), EValueType::String);
        columns.emplace_back(std::string(SystemTimestampFilterColumnName), EValueType::Uint64);
        columns.emplace_back(std::string(EventTimestampFilterColumnName), EValueType::Uint64);
        columns.emplace_back(std::string(AlignmentTimestampFilterColumnName), EValueType::Uint64);
        return New<TTableSchema>(std::move(columns));
    }
};

using TCompiledFilterPtr = TIntrusivePtr<TCompiledFilter>;

////////////////////////////////////////////////////////////////////////////////

class TMessageFilter
    : public IMessageFilter
{
public:
    explicit TMessageFilter(const std::optional<std::string>& skipIfExpression)
    {
        Reconfigure(skipIfExpression);
    }

    void Reconfigure(const std::optional<std::string>& skipIfExpression) override
    {
        std::string_view expression = skipIfExpression ? std::string_view(*skipIfExpression) : std::string_view();

        auto compiled = Compiled_.Acquire();
        std::string_view currentExpression = compiled ? compiled->GetExpression() : std::string_view();
        if (expression == currentExpression) {
            return;
        }

        Compiled_.Store(expression.empty()
                ? nullptr
                : New<TCompiledFilter>(std::string(expression)));
    }

    bool IsEnabled() const override
    {
        return static_cast<bool>(Compiled_.Acquire());
    }

    bool ShouldSkip(const TMessage& message) const override
    {
        auto compiled = Compiled_.Acquire();
        return compiled && compiled->ShouldSkip(message);
    }

    TMessageFilterResult Partition(std::vector<TInputMessageConstPtr> messages) const override
    {
        auto compiled = Compiled_.Acquire();

        TMessageFilterResult result;
        if (!compiled) {
            result.Kept = std::move(messages);
            return result;
        }

        result.Kept.reserve(messages.size());
        for (auto& message : messages) {
            if (compiled->ShouldSkip(*message)) {
                result.Skipped.push_back(std::move(message));
            } else {
                result.Kept.push_back(std::move(message));
            }
        }
        return result;
    }

private:
    TAtomicIntrusivePtr<TCompiledFilter> Compiled_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IMessageFilterPtr CreateMessageFilter(const std::optional<std::string>& skipIfExpression)
{
    return New<TMessageFilter>(skipIfExpression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
