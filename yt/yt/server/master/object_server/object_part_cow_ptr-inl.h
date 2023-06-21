#ifndef OBJECT_PART_COW_PTR_INL_H_
#error "Direct inclusion of this file is not allowed, include object_part_cow_ptr.h"
// For the sake of sane code completion.
#include "object_part_cow_ptr.h"
#endif

namespace NYT::NObjectServer {

///////////////////////////////////////////////////////////////////////////////

template <CCoWObject TObjectPart>
TObjectPart TObjectPartCoWPtr<TObjectPart>::DefaultObjectPart;

template <CCoWObject TObjectPart>
TObjectPartCoWPtr<TObjectPart>::~TObjectPartCoWPtr()
{
    Reset();
}

template <CCoWObject TObjectPart>
TObjectPartCoWPtr<TObjectPart>::operator bool() const
{
    return static_cast<bool>(ObjectPart_);
}

template <CCoWObject TObjectPart>
inline const TObjectPart& TObjectPartCoWPtr<TObjectPart>::Get() const
{
    return ObjectPart_ ? *ObjectPart_ : DefaultObjectPart;
}

template <CCoWObject TObjectPart>
TObjectPart& TObjectPartCoWPtr<TObjectPart>::MutableGet()
{
    MaybeCopyOnWrite();
    return *ObjectPart_;
}

template <CCoWObject TObjectPart>
void TObjectPartCoWPtr<TObjectPart>::Assign(const TObjectPartCoWPtr& rhs)
{
    if (ObjectPart_ == rhs.ObjectPart_) {
        return;
    }

    Reset();
    ObjectPart_ = rhs.ObjectPart_;

    if (ObjectPart_) {
        ObjectPart_->Ref();
    }
}

template <CCoWObject TObjectPart>
void TObjectPartCoWPtr<TObjectPart>::Reset()
{
    if (ObjectPart_) {
        ObjectPart_->Unref();
        if (ObjectPart_->GetRefCount() == 0) {
            delete ObjectPart_;
        }

        ObjectPart_ = nullptr;
    }
}

template <CCoWObject TObjectPart>
void TObjectPartCoWPtr<TObjectPart>::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    if (ObjectPart_) {
        auto key = context.RegisterRawEntity(ObjectPart_);
        Save(context, key);
        if (key == TEntityStreamSaveContext::InlineKey) {
            Save(context, *ObjectPart_);
        }
    } else {
        Save(context, TEntitySerializationKey());
    }
}

template <CCoWObject TObjectPart>
void TObjectPartCoWPtr<TObjectPart>::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    YT_VERIFY(!ObjectPart_);

    auto key = Load<TEntitySerializationKey>(context);
    if (!key) {
        SERIALIZATION_DUMP_WRITE(context, "objref <null>");
    } else if (key == TEntityStreamSaveContext::InlineKey) {
        ResetToDefaultConstructed();
        SERIALIZATION_DUMP_INDENT(context) {
            Load(context, *ObjectPart_);
        }
        auto loadedKey = context.RegisterRawEntity(ObjectPart_);
        SERIALIZATION_DUMP_WRITE(context, "objref %v", loadedKey.Index);
    } else {
        ObjectPart_ = context.template GetRawEntity<TObjectPart>(key);
        // NB: this only works iff the ref counter is embedded into the object.
        // This is essentially the same as re-wrapping raw ptrs into intrusive ones.
        ObjectPart_->Ref();
        SERIALIZATION_DUMP_WRITE(context, "objref %v", key.Index);
    }
}

template <CCoWObject TObjectPart>
void TObjectPartCoWPtr<TObjectPart>::MaybeCopyOnWrite()
{
    if (!ObjectPart_) {
        ResetToDefaultConstructed();
    } else if (ObjectPart_->GetRefCount() > 1) {
        auto* objectPartCopy = TObjectPart::Copy(ObjectPart_).release();
        objectPartCopy->Ref();

        ObjectPart_->Unref();
        ObjectPart_ = objectPartCopy;

        YT_VERIFY(ObjectPart_->GetRefCount() == 1);
    }

    YT_VERIFY(ObjectPart_ && ObjectPart_->GetRefCount() == 1);
}

template <CCoWObject TObjectPart>
void TObjectPartCoWPtr<TObjectPart>::ResetToDefaultConstructed()
{
    YT_VERIFY(!ObjectPart_);
    ObjectPart_ = new TObjectPart();
    ObjectPart_->Ref();
    YT_ASSERT(ObjectPart_->GetRefCount() == 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
