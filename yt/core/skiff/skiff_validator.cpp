#include "skiff.h"
#include "skiff_validator.h"

#include <yt/core/misc/error.h>

#include <stack>

namespace NYT {
namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

using TValidatorNodeList = std::vector<IValidatorNodePtr>;
using TSkiffSchemaList = std::vector<TSkiffSchemaPtr>;

static IValidatorNodePtr CreateUsageValidatorNode(TSkiffSchemaPtr skiffSchema);
static TValidatorNodeList CreateUsageValidatorNodeList(const TSkiffSchemaList& skiffSchemaList);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
inline void ThrowUnexpectedParseWrite(T wireType)
{
    THROW_ERROR_EXCEPTION("Unexpeceted parse/write of %Qv token",
        wireType);
}

////////////////////////////////////////////////////////////////////////////////

class IValidatorNode
    : public TRefCounted
{
public:
    virtual void OnBegin(TValidatorNodeStack* /*context*/)
    { }

    virtual void OnChildDone(TValidatorNodeStack* /*context*/)
    {
        Y_UNREACHABLE();
    }

    virtual void OnSimpleType(TValidatorNodeStack* /*context*/, EWireType wireType)
    {
        ThrowUnexpectedParseWrite(wireType);
    }

    virtual void BeforeVariant8Tag()
    {
        ThrowUnexpectedParseWrite(EWireType::Variant8);
    }

    virtual void OnVariant8Tag(TValidatorNodeStack* /*context*/, ui8 /*tag*/)
    {
        IValidatorNode::BeforeVariant8Tag();
    }

    virtual void BeforeVariant16Tag()
    {
        ThrowUnexpectedParseWrite(EWireType::Variant16);
    }

    virtual void OnVariant16Tag(TValidatorNodeStack* /*context*/, ui16 /*tag*/)
    {
        IValidatorNode::BeforeVariant16Tag();
    }
};

DEFINE_REFCOUNTED_TYPE(IValidatorNode);

////////////////////////////////////////////////////////////////////////////////

class TValidatorNodeStack
{
public:
    explicit TValidatorNodeStack(IValidatorNodePtr validator)
        : RootValidator_(validator)
    { }

    void PushValidator(IValidatorNode* validator)
    {
        ValidatorStack_.push(validator);
        validator->OnBegin(this);
    }

    void PopValidator()
    {
        YCHECK(!ValidatorStack_.empty());
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
        YCHECK(!ValidatorStack_.empty());
        return ValidatorStack_.top();
    }

    bool IsFinished() const
    {
        return ValidatorStack_.empty();
    }

private:
    std::stack<IValidatorNode*> ValidatorStack_;
    IValidatorNodePtr RootValidator_;
};

////////////////////////////////////////////////////////////////////////////////

class TNothingTypeValidator
    : public IValidatorNode
{
public:
    TNothingTypeValidator()
    { }

    void OnBegin(TValidatorNodeStack* context) override
    {
        context->PopValidator();
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

    virtual void OnSimpleType(TValidatorNodeStack* context, EWireType type) override
    {
        if (type != Type_) {
            ThrowUnexpectedParseWrite(type);
        }
        context->PopValidator();
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
        : Children_(children)
    { }

    void BeforeVariant8Tag() override
    { }

    void OnVariant8Tag(TValidatorNodeStack* context, ui8 tag) override
    {
        if (tag >= Children_.size()) {
            THROW_ERROR_EXCEPTION("Tag %Qv exceeds number of children: %Qv",
                tag,
                Children_.size());
        }
        context->PushValidator(Children_[tag].Get());
    }

    virtual void OnChildDone(TValidatorNodeStack* context) override
    {
        context->PopValidator();
    }

private:
    TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TVariant16TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TVariant16TypeUsageValidator(TValidatorNodeList children)
        : Children_(children)
    { }

    void BeforeVariant16Tag() override
    { }

    void OnVariant16Tag(TValidatorNodeStack* context, ui16 tag) override
    {
        if (tag >= Children_.size()) {
            THROW_ERROR_EXCEPTION("Tag %Qv exceeds number of children: %Qv",
                tag,
                Children_.size());
        }
        context->PushValidator(Children_[tag].Get());
    }

    virtual void OnChildDone(TValidatorNodeStack* context) override
    {
        context->PopValidator();
    }

private:
    TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TRepeatedVariant16TypeUsageValidator
    : public IValidatorNode
{
public:
    explicit TRepeatedVariant16TypeUsageValidator(TValidatorNodeList children)
        : Children_(children)
    { }

    void BeforeVariant16Tag() override
    { }

    void OnVariant16Tag(TValidatorNodeStack* context, ui16 tag) override
    {
        if (tag == EndOfSequenceTag<ui16>()) {
            context->PopValidator();
        } else if (tag >= Children_.size()) {
            THROW_ERROR_EXCEPTION("Tag %Qv exceeds number of children: %Qv",
                tag,
                Children_.size());
        } else {
            context->PushValidator(Children_[tag].Get());
        }
    }

    virtual void OnChildDone(TValidatorNodeStack* /*context*/) override
    { }

private:
    TValidatorNodeList Children_;
};

////////////////////////////////////////////////////////////////////////////////

class TTupleTypeUsageValidator
    : public IValidatorNode
{
public:

    explicit TTupleTypeUsageValidator(TValidatorNodeList children)
        : Children_(children)
    { }

    virtual void OnBegin(TValidatorNodeStack* context) override
    {
        Position_ = 0;
        context->PushValidator(Children_[0].Get());
    }

    virtual void OnChildDone(TValidatorNodeStack* context) override
    {
        Position_++;
        if (Position_ < Children_.size()) {
            context->PushValidator(Children_[Position_].Get());
        } else {
            context->PopValidator();
        }
    }

private:
    TValidatorNodeList Children_;
    ui32 Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TSkiffValidator::TSkiffValidator(TSkiffSchemaPtr skiffSchema)
    : Context_(std::make_unique<TValidatorNodeStack>(CreateUsageValidatorNode(skiffSchema)))
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
    Context_->Top()->OnVariant8Tag(Context_.get(), tag);
}

void TSkiffValidator::BeforeVariant16Tag()
{
    Context_->PushRootIfRequired();
    Context_->Top()->BeforeVariant16Tag();
}

void TSkiffValidator::OnVariant16Tag(ui16 tag)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnVariant16Tag(Context_.get(), tag);
}

void TSkiffValidator::OnSimpleType(EWireType value)
{
    Context_->PushRootIfRequired();
    Context_->Top()->OnSimpleType(Context_.get(), value);
}

void TSkiffValidator::ValidateFinished()
{
    if (!Context_->IsFinished()) {
        THROW_ERROR_EXCEPTION("Parse/write is not finished");
    }
}

////////////////////////////////////////////////////////////////////////////////

TValidatorNodeList CreateUsageValidatorNodeList(const TSkiffSchemaList& skiffSchemaList)
{
    TValidatorNodeList result;
    for (const auto& skiffSchema : skiffSchemaList)
    {
        result.push_back(CreateUsageValidatorNode(skiffSchema));
    }
    return result;
}

IValidatorNodePtr CreateUsageValidatorNode(TSkiffSchemaPtr skiffSchema)
{
    switch (skiffSchema->GetWireType()) {
        case EWireType::Yson32:
        case EWireType::Int64:
        case EWireType::Uint64:
        case EWireType::String32:
        case EWireType::Double:
        case EWireType::Boolean:
            return New<TSimpleTypeUsageValidator>(skiffSchema->GetWireType());
        case EWireType::Nothing:
            return New<TNothingTypeValidator>();
        case EWireType::Tuple:
            return New<TTupleTypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->AsTupleSchema()->GetChildren()));
        case EWireType::Variant8:
            return New<TVariant8TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->AsVariant8Schema()->GetChildren()));
        case EWireType::Variant16:
            return New<TVariant16TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->AsVariant16Schema()->GetChildren()));
        case EWireType::RepeatedVariant16:
            return New<TRepeatedVariant16TypeUsageValidator>(CreateUsageValidatorNodeList(skiffSchema->AsRepeatedVariant16Schema()->GetChildren()));
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
} // namespace NYT
