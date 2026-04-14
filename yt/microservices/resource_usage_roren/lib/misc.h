#pragma once

#include <library/cpp/yson/node/node.h>

#include <util/generic/fwd.h>
#include <util/system/yassert.h>

#include <concepts>
#include <type_traits>
#include <optional>
#include <algorithm>

TString LoadResourceUsageToken();

template <typename T>
struct member_pointer_traits;
template <typename Class, typename Value>
struct member_pointer_traits<Value Class::*> { typedef Class ctype; typedef Value vtype; };

template <typename T>
struct optional_traits{ typedef T underlying_type; };
template <typename T>
struct optional_traits<std::optional<T>> { typedef T underlying_type; };


template <typename T>
concept IsOptional = std::same_as<T, std::optional<typename T::value_type>>;


template <typename T>
void SetMin(std::optional<T>& to, const T& from) {
    if (!to.has_value()) {
        to = from;
    } else {
        to = std::min(to.value(), from);
    }
}

template <typename T>
void SetMin(std::optional<T>& to, const std::optional<T>& from) {
    if (!from.has_value()) {
        return;
    }
    if (!to.has_value()) {
        to = from.value();
    } else {
        to = std::min(to.value(), from.value());
    }
}


template <class TInput, class TOutput>
class IValueAggregator
{
public:
    virtual void Update(const TInput& v) = 0;
    virtual void Finish(TOutput& output) = 0;
    virtual ~IValueAggregator() = default;
};


template <class TSetter, class TField>
class TSetUniqueValue:
    public IValueAggregator<typename member_pointer_traits<TField>::ctype, typename member_pointer_traits<TSetter>::ctype>
{
public:
    using TOutput = typename member_pointer_traits<TSetter>::ctype;
    using TFieldClass = typename member_pointer_traits<TField>::ctype;
    using TFieldValue = typename member_pointer_traits<TField>::vtype;

    TSetUniqueValue(TSetter setter, TField field)
        : Setter_(std::move(setter))
        , Field_(std::move(field))
    { }

    void Update(const TFieldClass& v) override
    {
        if constexpr (IsOptional<TFieldValue>) {
            if (!Result_.has_value()) {
                if ((v.*Field_).has_value()) {
                    Result_ = (v.*Field_).value();
                }
            } else {
                if ((v.*Field_).has_value()) {
                    Y_ABORT_IF(Result_.value().value() != v.*Field_.value());
                }
            }
        } else {
            if (!Result_.has_value()) {
                Result_ = v.*Field_;
            } else {
                Y_ABORT_IF(Result_.value() != v.*Field_);
            }
        }
    }

    void Finish(TOutput& output) override
    {
        if (Result_.has_value()) {
            (output.*Setter_)(Result_.value());
        }
    }

private:
    TSetter Setter_;
    TField Field_;
    std::optional<typename optional_traits<TFieldValue>::underlying_type> Result_;
};


template <class TSetter, class TField>
class TSetNonTxOrAnyValue:
    public IValueAggregator<typename member_pointer_traits<TField>::ctype, typename member_pointer_traits<TSetter>::ctype>
{
public:
    using TOutput = typename member_pointer_traits<TSetter>::ctype;
    using TFieldClass = typename member_pointer_traits<TField>::ctype;
    using TFieldValue = typename member_pointer_traits<TField>::vtype;

    TSetNonTxOrAnyValue(TSetter setter, TField field)
        : Setter_(std::move(setter))
        , Field_(std::move(field))
    { }

    void Update(const TFieldClass& v) override
    {
        if (v.CypressTransactionId.has_value()) {
            SetMin(Any_, v.*Field_);
        } else {
            SetMin(Result_, v.*Field_);
        }
    }

    void Finish(TOutput& output) override
    {
        if (Result_.has_value()) {
            (output.*Setter_)(Result_.value());
        } else if (Any_.has_value()) {
            (output.*Setter_)(Any_.value());
        }
    }

private:
    TSetter Setter_;
    TField Field_;
    std::optional<typename optional_traits<TFieldValue>::underlying_type> Result_;
    std::optional<typename optional_traits<TFieldValue>::underlying_type> Any_;
};


template <class TInput, class TOutput>
class TMutiAggregator
{
public:
    TMutiAggregator(TOutput& out)
        : Output_(out)
    { }

    ~TMutiAggregator()
    {
        if (!Finished_) {
            Finish();
        }
    }

    template <class TSetter, class TField>
    void SetUnique(TSetter setter, TField field)
    {
        Aggregators_.push_back(new TSetUniqueValue<TSetter, TField>(std::move(setter), std::move(field)));
    }

    template <class TSetter, class TField>
    void SetNonTxOrAny(TSetter setter, TField field)
    {
        Aggregators_.push_back(new TSetNonTxOrAnyValue<TSetter, TField>(std::move(setter), std::move(field)));
    }

    void Update(const TInput& val)
    {
        for (auto aggregator : Aggregators_) {
            aggregator->Update(val);
        }
    }

    void Finish()
    {
        Finished_ = true;
        for (auto aggregator : Aggregators_) {
            aggregator->Finish(Output_);
        }
    }

public:
    bool Finished_ = false;
    TOutput& Output_;
    std::vector<IValueAggregator<TInput, TOutput>*> Aggregators_;
};

template <class TField, class TMerger>
class TMergeAggregator
{
public:
    using TFieldClass = typename member_pointer_traits<TField>::ctype;
    using TFieldValue = typename member_pointer_traits<TField>::vtype;

    TMergeAggregator(TField field, TMerger merger)
        : Field_(std::move(field))
        , Merger_(std::move(merger))
    { }

    void Update(const TFieldClass& row)
    {
        if (!Result_.has_value()) {
            Result_ = row.*Field_;
        } else {
            Merger_(Result_.value(), row.*Field_);
        }
    }

    template <class TOutput, class TSetter>
    void Finish(TOutput& out, TSetter& setter)
    {
        if (Result_.has_value()) {
            setter(out, Result_.value());
        }
    }

    TFieldValue& GetResult()
    {
        Y_ABORT_IF(!Result_.has_value());
        return Result_.value();
    }

    const TFieldValue& GetResult() const
    {
        Y_ABORT_IF(!Result_.has_value());
        return Result_.value();
    }

    bool HasValue() const
    {
        return Result_.has_value();
    }

private:
    std::optional<TFieldValue> Result_;
    TField Field_;
    TMerger Merger_;
};

void MergeIntNodeWith(NYT::TNode& to, const NYT::TNode& from);
i64 GetInt64FromTNode(const NYT::TNode& node, const TStringBuf key);

struct TVersionedResourceUsage
{
    TString TransactionTitle;
    NYT::TNode VersionedResourceUsageNode;
    NYT::TNode DiskSpacePerMedium;
    bool IsOriginal;

    Y_SAVELOAD_DEFINE(
        TransactionTitle,
        VersionedResourceUsageNode,
        DiskSpacePerMedium,
        IsOriginal);

    TVersionedResourceUsage();
    TVersionedResourceUsage(TString title, NYT::TNode vrun, NYT::TNode dspm, bool IsOriginal);
    void MergeWith(const TVersionedResourceUsage& from);
    NYT::TNode ToNode() const;
};

using TVersionedResourceUsageMap = THashMap<TString, TVersionedResourceUsage>;

NYT::TNode VersionedResourceUsageTop(const TVersionedResourceUsageMap& vrum, ui64 topN);
