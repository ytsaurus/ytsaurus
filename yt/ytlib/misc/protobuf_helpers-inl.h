#ifndef PROTOBUF_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include protobuf_helpers.h"
#endif
#undef PROTOBUF_HELPERS_INL_H_

#include "foreach.h"
#include "assert.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T GetProtoExtension(const NProto::TExtensionSet& extensions)
{
    // Intentionally complex to take benefit of RVO.
    T result;
    i32 tag = GetProtoExtensionTag<T>();
    bool found = false;
    FOREACH (const auto& extension, extensions.extensions()) {
        if (extension.tag() == tag) {
            const auto& data = extension.data();
            result.ParseFromArray(data.begin(), data.length());
            found = true;
            break;
        }
    }
    YCHECK(found);
    return result;
}

template <class T>
TNullable<T> FindProtoExtension(const NProto::TExtensionSet& extensions)
{
    TNullable<T> result;
    i32 tag = GetProtoExtensionTag<T>();
    FOREACH (const auto& extension, extensions.extensions()) {
        if (extension.tag() == tag) {
            const auto& data = extension.data();
            result.Assign(T());
            result.Get().ParseFromArray(data.begin(), data.length());
            break;
        }
    }
    return result;
}

template <class T>
void SetProtoExtension(NProto::TExtensionSet* extensions, const T& value)
{
    i32 tag = GetProtoExtensionTag<T>();
    FOREACH (const auto& currentExtension, extensions->extensions()) {
        YASSERT(currentExtension.tag() != tag);
    }

    auto extension = extensions->add_extensions();
    int size = value.ByteSize();
    Stroka str(size);
    YVERIFY(value.SerializeToArray(str.begin(), size));
    extension->set_data(str);
    extension->set_tag(tag);
}

template <class T>
void UpdateProtoExtension(NProto::TExtensionSet* extensions, const T& value)
{
    i32 tag = GetProtoExtensionTag<T>();
    NYT::NProto::TExtension* extension = NULL;
    FOREACH (auto& currentExtension, *extensions->mutable_extensions()) {
        if (currentExtension.tag() == tag) {
            extension = &currentExtension;
            break;
        }
    }
    if (!extension) {
        extension = extensions->add_extensions();
    }

    int size = value.ByteSize();
    Stroka str(size);
    YVERIFY(value.SerializeToArray(str.begin(), size));
    extension->set_data(str);
    extension->set_tag(tag);
}

template <class T>
bool RemoveProtoExtension(NProto::TExtensionSet* extensions)
{
    i32 tag = GetProtoExtensionTag<T>();
    for (int index = 0; index < extensions->extensions_size(); ++index) {
        const auto& currentExtension = extensions->extensions(index);
        if (currentExtension.tag() == tag) {
            // Make it the last one.
            extensions->mutable_extensions()->SwapElements(index, extensions->extensions_size() - 1);
            // And then drop.
            extensions->mutable_extensions()->RemoveLast();
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
