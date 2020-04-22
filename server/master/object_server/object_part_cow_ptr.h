#pragma once
#include "public.h"

namespace NYT::NObjectServer {

///////////////////////////////////////////////////////////////////////////////

//! A device for making parts of TObjects copy-on-write. Essentially, this is a
//! smart(ish) pointer with CoW support. It also supports saveloading.
/*!
 *  In order to imbue a part of a TObject with CoW magic, one must segregate
 *  that part into a class - named TObjectPart here. TObjectPart is not required
 *  to (and, indeed, is not expected to) be itself a TObject.
 *
 *  The pointer makes no distinction between null and default-constructed
 *  value. By default, internally, it's null. Read-dereferencing (via #Get()) a
 *  null pointer is allowed and will result in a (const) reference to a (static)
 *  default-constructed object. Write-dereferencing (via #MutableGet()) a null
 *  pointer will default-construct an object and return a reference to it.
 *
 *  Two pointers may refer to the same object part if one of them is assigned to
 *  the other via #Assign. Both such pointers may be used to read-access the
 *  object part, but attempting to write-access any of them will result in
 *  copying and splitting the pointers.
 *
 *  TObjectPart must be intrusive ref-countable and must implement the following
 *  methods:
 *    - refcounting: Ref(), Unref(), GetRefCount();
 *    - saveloading: Save(context), Load(context);
 *    - lifetime management:
 *        default ctor;
 *        static void Destroy(objectPart, objectManager);
 *        static TObjectPart* Copy(objectPart, objectManager).
 *
 *  Default ctor and Copy() must return a new TObjectPart with 0 ref-counter. If
 *  the ref-counter falls to 0 (during a call to #Reset()), #Destroy() is
 *  called.  The #objectManager argument may come useful if TObjectPart owns
 *  other TObjects and must ref/unref them when necessary.
 */
template <class TObjectPart>
class TObjectPartCoWPtr
{
public:
    TObjectPartCoWPtr() = default;

    TObjectPartCoWPtr(const TObjectPartCoWPtr&) = delete;
    TObjectPartCoWPtr& operator=(const TObjectPartCoWPtr&) = delete;

    ~TObjectPartCoWPtr();

    explicit operator bool() const;

    const TObjectPart& Get() const;
    TObjectPart& MutableGet(const NObjectServer::TObjectManagerPtr& objectManager);

    void Assign(const TObjectPartCoWPtr& rhs, const NObjectServer::TObjectManagerPtr& objectManager);

    //! Unrefs the part and calls TObjectPart::Destroy if refcounter becomes zero.
    //! Must be called when the object of which this is a part of is destroyed.
    //! NB: Either Reset or Clear must be called before the destructor.
    void Reset(const NObjectServer::TObjectManagerPtr& objectManager);

    //! Unrefs the part and calls TObjectPart::Clear if refcounter becomes zero.
    //! NB: Either Reset or Clear must be called before the destructor.
    void Clear();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // COMPAT(shakurov): make this method private once it's no longer used in the compat code.
    void ResetToDefaultConstructed();

private:
    TObjectPart* ObjectPart_ = nullptr;

    static TObjectPart DefaultObjectPart;

    void MaybeCopyOnWrite(const NObjectServer::TObjectManagerPtr& objectManager);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define OBJECT_PART_COW_PTR_INL_H_
#include "object_part_cow_ptr-inl.h"
#undef OBJECT_PART_COW_PTR_INL_H_
