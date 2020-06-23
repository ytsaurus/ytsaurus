#include "skiff_validator.h"

#include <util/generic/stack.h>
#include <util/generic/vector.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

struct IValidatorNode;
using IValidatorNodePtr = TIntrusivePtr<IValidatorNode>;

using TValidatorNodeList = TVector<IValidatorNodePtr>;
using TSkiffSchemaList = TVector<TSkiffSchemaPtr>;

static IValidatorNodePtr CreateUsageValidatorNode(const TSkiffSchemaPtr& skiffSchema);
static TValidatorNodeList CreateUsageValidatorNodeList(const TSkiffSchemaList& skiffSchemaList);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
inline void ThrowUnexpectedParseWrite(T wireType)
{
    ythrow yexception() << "Unexpected parse/write of " << wireType << " token";
}

////////////////////////////////////////////////////////////////////////////////

struct IValidatorNode
    : public TThrRefBase
{
    virtual void OnBegin(TValidatorNodeStack* /*validatorNodeStack*/)
    { }

    virtual void OnChildDone(TValidatorNodeStack* /*validatorNodeStack*/)
    {
        Y_FAIL("Unreachable");
    }

    virtual void OnSimpleType(TValidatorNodeStack* /*validatorNodeStack*/, EWireType wireType)
    {
        ThrowUnexpectedParseWrite(wireType);
    }

    virtual void BeforeVariant8Tag()
    {
        ThrowUnexpectedParseWrite(EWireType::Variant8);
    }

    virtual void OnVariant8Tag(TValidatorNodeStack* /*validatorNodeStack*/, ui8 /*tag*/)
    {
        IValidatorNode::BeforeVariant8Tag();
    }

    virtual void BeforeVariant16Tag()
    {
        ThrowUnexpectedParseWrite(EWireType::Variant16);
    }

    virtual void OnVariant16Tag(TValidatorNodeStack* /*validatorNodeStack*/, ui16 /*tag*/)
    {
        IValidatorNode::BeforeVariant16Tag();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TValidatorNodeStack
{
public:
    explicit TValidatorNodeStack(IValidatorNodePtr validator)
        : RootValidator_(std::move(validator))
    { }

    void PushValidator(IValidatorNode* validator)
    {
        ValidatorStack_.push(validator);
        validator->OnBegin(this);
    }

    void PopValidator()
    {
        Y_ENSURE(!ValidatorStack_.empty());
        ValidatorStack_.pop();
        if (!ValidatorStack_.empty()) {
            ValidatorStack_.top()->OnChildDone(this);
        }
    }

    void PushRootIfRequired()
    {
        if (ValidatorStack_.empty()) {
            PushValidator(RootValidator_.Get());
        }
    }

    IValidatorNode* Top() const
    {
        Y_ENSURE(!ValidatorStack_.empty());
        return ValidatorStack_.top();
    }

    bool IsFinished() const
    {
        return ValidatorStack_.empty();
    }

private:
    const IValidatorNodePtr RootValidator_;
    TStack<IValidatorNode*> ValidatorStack_;
};

////////////////////////////////////////////////////////////////////////////////

class TNothingTypeValidator
    : public IValidatorNode
{
public:
    virtual void OnBegin(TValidatorNodeStack* validatorNodeStack) override
    {
        validatorNodeStack->PopValidator();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TSimpleTypeUsageValidator(EWireType type)
        : Type_(type)
    { }

    virtual void OnSimpleType(TValidatorNodeStack* validatorNodeStack, EWireType type) override
    {
        if (type != Type_) {
            ThrowUnexpectedParseWrite(type);
        }
        validatorNodeStack->PopValidator();
    }

private:
    const EWireType Type_;
};

////////////////////////////////////////////////////////////////////////////////

class TVariant8TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TVariant8TypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    virtual void BeforeVariant8Tag() override
    { }

    virtual void OnVariant8Tag(TValidatorNodeStack* validatorNodeStack, ui8 tag) override
    {
        if (tag >= Children_.size()) {
            ythrow yexception() << "Variant tag " << tag << " exceeds number of children " << Children_.size();
        }
        validatorNodeStack->PushValidator(Children_[tag].Get());
    }

    virtual void OnChildDone(TValidatorNodeStack* validatorNodeStack) override
    {
        validatorNodeStack->PopValidator();
    }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TVariant16TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TVariant16TypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    virtual void BeforeVariant16Tag() override
    { }

    virtual void OnVariant16Tag(TValidatorNodeStack* validatorNodeStack, ui16 tag) override
    {
        if (tag >= Children_.size()) {
            ythrow yexception() << "Variant tag " << tag << " exceeds number of children " << Children_.size();
        }
        validatorNodeStack->PushValidator(Children_[tag].Get());
    }

    virtual void OnChildDone(TValidatorNodeStack* validatorNodeStack) override
    {
        validatorNodeStack->PopValidator();
    }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TRepeatedVariant8TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TRepeatedVariant8TypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    virtual void BeforeVariant8Tag() override
    { }

    virtual void OnVariant8Tag(TValidatorNodeStack* validatorNodeStack, ui8 tag) override
    {
        if (tag == EndOfSequenceTag<ui8>()) {
            validatorNodeStack->PopValidator();
        } else if (tag >= Children_.size()) {
            ythrow yexception() << "Variant tag " << tag << " exceeds number of children " << Children_.size();
        } else {
            validatorNodeStack->PushValidator(Children_[tag].Get());
        }
    }

    virtual void OnChildDone(TValidatorNodeStack* /*validatorNodeStack*/) override
    { }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TRepeatedVariant16TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TRepeatedVariant16TypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    virtual void BeforeVariant16Tag() override
    { }

    virtual void OnVariant16Tag(TValidatorNodeStack* validatorNodeStack, ui16 tag) override
    {
        if (tag == EndOfSequenceTag<ui16>()) {
            validatorNodeStack->PopValidator();
        } else if (tag >= Children_.size()) {
            ythrow yexception() << "Variant tag " << tag << " exceeds number of children " << Children_.size();
        } else {
            validatorNodeStack->PushValidator(Children_[tag].Get());
        }
    }

    virtual void OnChildDone(TValidatorNodeStack* /*validatorNodeStack*/) override
    { }

private:
    const TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TTupleTypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TTupleTypeUsageValidator(TValidatorNodeList children)
        : Children_(std::move(children))
    { }

    virtual void OnBegin(TValidatorNodeStack* validatorNodeStack) override
    {
        Position_ = 0;
        validatorNodeStack->PushValidator(Children_[0].Get());
    }

    virtual void OnChildDone(TValidatorNodeStack* validatorNodeStack) override
    {
        Position_++;
        if (Position_ < Children_.size()) {
            validatorNodeStack->PushValidator(Children_[Position_].Get());
        } else {
            validatorNodeStack->PopValidator();
        }
    }

private:
    const TValidatorNodeList Children_;
    ui32 Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TSkiffValidator::TSkiffValidator(TSkiffSchemaPtr skiffSchema)
    : Context_(::MakeHolder<TValidatorNodeStack>(CreateUsageValidatorNode(std::move(skiffSchema))))
{ }

TSkiffValidator::~TSkiffValidator()
{ }

void TSkiffValidator::BeforeVariant8Tag()
{
    Context_->PushRootIfRequired();
    Context_->Top()->BeforeVariant8Tag();
}

void TSkiffValidator::OnVariant8Tag(ui8 tag)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnVariant8Tag(Context_.Get(), tag);
}

void TSkiffValidator::BeforeVariant16Tag()
{
    Context_->PushRootIfRequired();
    Context_->Top()->BeforeVariant16Tag();
}

void TSkiffValidator::OnVariant16Tag(ui16 tag)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnVariant16Tag(Context_.Get(), tag);
}

void TSkiffValidator::OnSimpleType(EWireType value)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnSimpleType(Context_.Get(), value);
}

void TSkiffValidator::ValidateFinished()
{
    Y_ENSURE(Context_->IsFinished(), "Parse/write is not finished");
}

////////////////////////////////////////////////////////////////////////////////

TValidatorNodeList CreateUsageValidatorNodeList(const TSkiffSchemaList& skiffSchemaList)
{
    TValidatorNodeList result;
    result.reserve(skiffSchemaList.size());
    for (const auto& skiffSchema : skiffSchemaList) {
        result.push_back(CreateUsageValidatorNode(skiffSchema));
    }
    return result;
}

IValidatorNodePtr CreateUsageValidatorNode(const TSkiffSchemaPtr& skiffSchema)
{
    switch (skiffSchema->GetWireType()) {
        case EWireType::Yson32:
        case EWireType::Int64:
        case EWireType::Uint64:
        case EWireType::String32:
        case EWireType::Double:
        case EWireType::Boolean:
            return ::MakeIntrusive<TSimpleTypeUsageValidator>(skiffSchema->GetWireType());
        case EWireType::Nothing:
            return ::MakeIntrusive<TNothingTypeValidator>();
        case EWireType::Tuple:
            return ::MakeIntrusive<TTupleTypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::Variant8:
            return ::MakeIntrusive<TVariant8TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::Variant16:
            return ::MakeIntrusive<TVariant16TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::RepeatedVariant8:
            return ::MakeIntrusive<TRepeatedVariant8TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
        case EWireType::RepeatedVariant16:
            return ::MakeIntrusive<TRepeatedVariant16TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->GetChildren()));
    }
    Y_FAIL("Unknown EWireType %d", static_cast<int>(skiffSchema->GetWireType()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
