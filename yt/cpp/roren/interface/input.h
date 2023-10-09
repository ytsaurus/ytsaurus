#pragma once

#include "fwd.h"

#include "coder.h"
#include "row.h"
#include "type_tag.h"
#include "private/raw_data_flow.h"
#include "private/statically_buffered_stream.h"

#include <util/generic/maybe.h>

#include <optional>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TInputStdIterator;

namespace NPrivate {
TMultiInputPtr CreateMultiInput(const std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>>& taggedInputs);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TRowType_>
class TInput
    : private NPrivate::IRawInput
{
public:
    using TRowType = TRowType_;

public:
    TInput() = delete;

    Y_FORCE_INLINE const TRowType* Next()
    {
        static_assert(sizeof(*this) == sizeof(NPrivate::IRawOutput));

        return static_cast<const TRowType*>(NextRaw());
    }

    // NB. This class is thin typed wrapper around IRawInput it allows compiler to type check program.
    // This class MUST NOT contain any data members.
};

template <>
class TInput<TMultiRow>
    : public TThrRefBase
{
public:
    TInput() = default;

    std::vector<TString> GetInputDescriptionList() const
    {
        std::vector<TString> result;
        for (const auto& tag : Tags_) {
            result.push_back(tag.GetDescription());
        }
        return result;
    }

    template <CRow TRow>
    TInput<TRow>& GetInput(TTypeTag<TRow> tag)
    {
        TKey key = TDynamicTypeTag(tag).GetKey();
        auto it = InputMap_.find(key);
        if (it == InputMap_.end()) {
            Y_FAIL("Unknown tag: %s", tag.GetDescription().c_str());
        }
        return *it->second->template Upcast<TRow>();
    }

private:
    using TKey = TDynamicTypeTag::TKey;

    THashMap<TKey, NPrivate::IRawInputPtr> InputMap_;
    std::vector<TDynamicTypeTag> Tags_;

    friend class TCoder<TMultiInputPtr>;
    friend TMultiInputPtr NPrivate::CreateMultiInput(const std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>>& taggedInputs);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRowType_>
class TInputPtr
{
public:
    using TRowType = TRowType_;
    using TValueType = TInput<TRowType>;

public:
    TInputPtr() = default;

    TInputPtr(std::nullptr_t)
    { }

    explicit TInputPtr(NPrivate::IRawInputPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    TInput<TRowType>& operator*() const
    {
        return *Underlying_->Upcast<TRowType>();
    }

    TInput<TRowType>* operator->() const
    {
        return Underlying_->Upcast<TRowType>();
    }

    TInput<TRowType>* Get() const
    {
        return Underlying_->Upcast<TRowType>();
    }

private:
    NPrivate::IRawInputPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::vector<T> ReadAllRows(TInput<T>& input)
{
    std::vector<T> result;
    while (const auto* t = input.Next()) {
        result.push_back(*t);
    }
    return result;
}

template <typename T>
T ReadSingleRow(TInput<T>& input)
{
    const auto* current = input.Next();
    Y_ABORT_UNLESS(current, "Input is empty");
    T result = std::move(*current);

    current = input.Next();
    Y_ABORT_UNLESS(current, "Input contains more than single entry");

    return result;
}

template <typename T>
std::optional<T> ReadOptionalRow(TInput<T>& input)
{
    const auto* current = input.Next();
    if (!current) {
        return {};
    }
    std::optional<T> result = *current;

    current = input.Next();
    Y_ABORT_UNLESS(!current, "Input contains more than single entry");

    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
NPrivate::IRawInputPtr MakeRawVectorInput(std::vector<T> rows);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TCoder<TInputPtr<T>>
{
public:
    inline void Encode(IOutputStream* out, const TInputPtr<T>& iter)
    {
        for (const auto& row : iter) {
            ::Save(out, true);
            ItemCoder_.Encode(out, row);
        }
        ::Save(out, false);
    }

    inline void Decode(IInputStream* in, TInputPtr<T>& iter)
    {
        bool ok;
        std::vector<T> rows;
        while (true) {
            ::Load(in, ok);
            if (!ok) {
                break;
            }
            rows.emplace_back();
            ItemCoder_.Decode(in, rows.back());
        }
        iter = TInputPtr<T>{MakeRawVectorInput(std::move(rows))};
    }

private:
    TCoder<T> ItemCoder_;
};

template <>
class TCoder<TMultiInputPtr>
{
public:
    inline void Encode(IOutputStream* outParam, const TMultiInputPtr& value)
    {
        auto out = NPrivate::TStaticallyBufferedStream{outParam};
        TagCoder_.Encode(&out, value->Tags_);

        InitRawCodersIfRequired(value->Tags_);

        TStringStream coded;
        for (ssize_t i = 0; i < ssize(value->Tags_); ++i) {
            const auto& tag = value->Tags_[i];
            const auto& coder = RawCoders_[i];
            const auto& input = value->InputMap_.at(tag.GetKey());
            while (const auto* row = input->NextRaw()) {
                ::Save(&out, true);
                coder->EncodeRow(&coded, row);
                ::Save(&out, coded.Str());
                coded.Str().clear();
            }
            ::Save(&out, false);
        }
        out.Finish();
    }

    inline void Decode(IInputStream* in, TMultiInputPtr& value)
    {
        value = ::MakeIntrusive<TMultiInput>();

        TagCoder_.Decode(in, value->Tags_);

        InitRawCodersIfRequired(value->Tags_);

        TString buffer;
        for (ssize_t i = 0; i < ssize(value->Tags_); ++i) {
            const auto& tag = value->Tags_[i];
            const auto& coder = RawCoders_[i];
            const auto& rowVtable = tag.GetRowVtable();

            std::vector<TString> rawValues;
            while (true) {
                bool hasNext;
                ::Load(in, hasNext);
                if (!hasNext) {
                    break;
                }
                ::Load(in, buffer);
                rawValues.emplace_back(buffer);
            }
            value->InputMap_.emplace(tag.GetKey(), ::MakeIntrusive<TSingleInput>(coder, std::move(rawValues), rowVtable));
        }
    }

private:
    void InitRawCodersIfRequired(const std::vector<TDynamicTypeTag>& tags)
    {
        if (RawCoders_.empty()) {
            for (const auto& tag: tags) {
                auto coder = tag.GetRowVtable().RawCoderFactory();
                RawCoders_.push_back(coder);
            }
        } else {
            Y_ABORT_UNLESS(RawCoders_.size() == tags.size());
        }
    }

    class TSingleInput
        : public NPrivate::IRawInput
    {
    public:
        TSingleInput(NPrivate::IRawCoderPtr coder, std::vector<TString> data, const NPrivate::TRowVtable& vtable)
            : Coder_(std::move(coder))
            , Data_(std::move(data))
            , RowHolder_(vtable)
        { }

        const void* NextRaw() override
        {
            if (Current_ == Data_.end()) {
                return nullptr;
            }
            Coder_->DecodeRow(*Current_, RowHolder_.GetData());
            ++Current_;
            return RowHolder_.GetData();
        }

    private:
        const NPrivate::IRawCoderPtr Coder_;
        const std::vector<TString> Data_;
        std::vector<TString>::const_iterator Current_ = Data_.begin();
        NPrivate::TRawRowHolder RowHolder_;
    };

private:
    TCoder<std::vector<TDynamicTypeTag>> TagCoder_;
    std::vector<NPrivate::IRawCoderPtr> RawCoders_;
};

template <typename TRowType_>
class TInputStdIterator
{
public:
    using TRowType = TRowType_;

public:
    TInputStdIterator() = default;

    explicit TInputStdIterator(TInput<TRowType>* input)
        : Input_(input)
        , Current_(Input_->Next())
    { }

    TInputStdIterator(TInputStdIterator<TRowType>&& other) noexcept = default;

    TInputStdIterator& operator++()
    {
        Current_ = Input_->Next();
        return *this;
    }

    const TRowType& operator*() const
    {
        return *Current_;
    }

    bool operator==(const TInputStdIterator<TRowType>& other) const
    {
        if (this == &other) {
            return true;
        }

        return !Current_ && !other.Current_;
    }

private:
    TInput<TRowType>* Input_ = nullptr;
    const TRowType* Current_ = nullptr;
};

template <typename T>
TInputStdIterator<T> begin(const TInputPtr<T>& iterator)
{
    return TInputStdIterator<T>{iterator.Get()};
}

template <typename T>
TInputStdIterator<T> end(TInputPtr<T>)
{
    return {};
}

template <typename T>
TInputStdIterator<T> begin(TInput<T>& input)
{
    return TInputStdIterator<T>{&input};
}

template <typename T>
TInputStdIterator<T> end(TInput<T>&)
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TRawVectorIterator
    : public NPrivate::IRawInput
{
public:
    explicit TRawVectorIterator(std::vector<T> rows)
        : Rows_(std::move(rows))
        , Iterator_(Rows_.begin())
    { }

    const void* NextRaw() override
    {
        if (Iterator_ == Rows_.end()) {
            return nullptr;
        } else {
            return &*(Iterator_++);
        }
    }

    void Reset(std::vector<T> rows)
    {
        Rows_ = std::move(rows);
        Iterator_ = Rows_.begin();
    }

private:
    std::vector<T> Rows_;
    typename std::vector<T>::const_iterator Iterator_;
};

template <typename T>
NPrivate::IRawInputPtr MakeRawVectorInput(std::vector<T> rows)
{
    return ::MakeIntrusive<TRawVectorIterator<T>>(std::move(rows));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
