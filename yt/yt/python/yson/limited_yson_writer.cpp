#include "limited_yson_writer.h"

#include <yt/core/yson/writer.h>

namespace NYT::NPython {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETokenType,
    (Attributes)
    (Map)
    (List)
)

class TLimitedYsonWriter::TImpl
    : public TRefCounted
{
public:
    TImpl(i64 limit, EYsonFormat ysonFormat)
        : Limit_(limit)
        , OutputStream_(Result_)
        , Writer_(&OutputStream_, ysonFormat)
    { }

    const TString& GetResult() const
    {
        return Result_;
    }

    void OnStringScalar(TStringBuf value)
    {
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnStringScalar(value.substr(0, std::min(static_cast<i64>(value.size()), static_cast<i64>(Limit_ - Result_.size()))));
        Postprocess();
    }

    void OnInt64Scalar(i64 value)
    {
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnInt64Scalar(value);
        Postprocess();
    }

    void OnUint64Scalar(ui64 value)
    {
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnUint64Scalar(value);
        Postprocess();
    }

    void OnDoubleScalar(double value)
    {
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnDoubleScalar(value);
        Postprocess();
    }

    void OnBooleanScalar(bool value)
    {
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnBooleanScalar(value);
        Postprocess();
    }

    void OnEntity()
    {
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnEntity();
        Postprocess();
    }

    void OnBeginList()
    {
        Stack_.push_back(std::make_pair(ETokenType::List, !LimitReached_));
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnBeginList();
        Postprocess();
    }

    void OnListItem()
    {
        if (LimitReached_) {
            return;
        }
        Writer_.OnListItem();
        Postprocess();
    }

    void OnEndList()
    {
        auto item = Stack_.back();
        YT_VERIFY(item.first == ETokenType::List);
        if (item.second) {
            Writer_.OnEndList();
        }
        Stack_.pop_back();
        Postprocess();
    }

    void OnBeginMap()
    {
        Stack_.push_back(std::make_pair(ETokenType::Map, !LimitReached_));
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnBeginMap();
        Postprocess();
    }

    void OnKeyedItem(TStringBuf name)
    {
        if (LimitReached_) {
            return;
        }
        Writer_.OnKeyedItem(name.substr(0, std::min(static_cast<i64>(name.size()), static_cast<i64>(Limit_ - Result_.size()))));
        // NB: Postprocess intentionally is not performed.
    }

    void OnEndMap()
    {
        auto item = Stack_.back();
        YT_VERIFY(item.first == ETokenType::Map);
        if (item.second) {
            Writer_.OnEndMap();
        }
        Stack_.pop_back();
        Postprocess();
    }

    void OnBeginAttributes()
    {
        Stack_.push_back(std::make_pair(ETokenType::Attributes, !LimitReached_));
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnBeginAttributes();
        Postprocess();
    }

    void OnEndAttributes()
    {
        auto item = Stack_.back();
        YT_VERIFY(item.first == ETokenType::Attributes);
        if (item.second) {
            Writer_.OnEndAttributes();
            ExpectValue_ = true;
        }
        Stack_.pop_back();
        // NB: Postprocess intentionally is not performed.
    }

    void OnRaw(TStringBuf yson, EYsonType type)
    {
        if (LimitReached_ && !ExpectValue_) {
            return;
        }
        Writer_.OnRaw(yson, type);
    }

private:
    i64 Limit_;

    std::vector<std::pair<ETokenType, bool>> Stack_;

    bool LimitReached_ = false;
    bool ExpectValue_ = false;

    TString Result_;
    TStringOutput OutputStream_;
    TYsonWriter Writer_;

    void Postprocess()
    {
        if (Result_.size() >= Limit_) {
            LimitReached_ = true;
        }
        ExpectValue_ = false;
    }
};

////////////////////////////////////////////////////////////////////////////////

TLimitedYsonWriter::TLimitedYsonWriter(i64 limit, EYsonFormat ysonFormat)
    : Impl_(New<TImpl>(limit, ysonFormat))
{ }

TLimitedYsonWriter::~TLimitedYsonWriter() = default;

const TString& TLimitedYsonWriter::GetResult() const
{
    return Impl_->GetResult();
}

void TLimitedYsonWriter::OnStringScalar(TStringBuf value)
{
    Impl_->OnStringScalar(value);
}

void TLimitedYsonWriter::OnInt64Scalar(i64 value)
{
    Impl_->OnInt64Scalar(value);
}

void TLimitedYsonWriter::OnUint64Scalar(ui64 value)
{
    Impl_->OnUint64Scalar(value);
}

void TLimitedYsonWriter::OnDoubleScalar(double value)
{
    Impl_->OnDoubleScalar(value);
}

void TLimitedYsonWriter::OnBooleanScalar(bool value)
{
    Impl_->OnBooleanScalar(value);
}

void TLimitedYsonWriter::OnEntity()
{
    Impl_->OnEntity();
}

void TLimitedYsonWriter::OnBeginList()
{
    Impl_->OnBeginList();
}

void TLimitedYsonWriter::OnListItem()
{
    Impl_->OnListItem();
}

void TLimitedYsonWriter::OnEndList()
{
    Impl_->OnEndList();
}

void TLimitedYsonWriter::OnBeginMap()
{
    Impl_->OnBeginMap();
}

void TLimitedYsonWriter::OnKeyedItem(TStringBuf name)
{
    Impl_->OnKeyedItem(name);
}

void TLimitedYsonWriter::OnEndMap()
{
    Impl_->OnEndMap();
}

void TLimitedYsonWriter::OnBeginAttributes()
{
    Impl_->OnBeginAttributes();
}

void TLimitedYsonWriter::OnEndAttributes()
{
    Impl_->OnEndAttributes();
}

void TLimitedYsonWriter::OnRaw(TStringBuf yson, EYsonType type)
{
    Impl_->OnRaw(yson, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
