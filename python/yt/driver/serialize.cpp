#include "common.h"
#include "serialize.h"

#include <core/yson/consumer.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TPythonYTreeProducer
{
public:
    explicit TPythonYTreeProducer(IYsonConsumer* consumer)
        : Consumer_(consumer)
    { }

    // TODO(babenko): Visit?
    void Process(const Py::Object& obj)
    {
        if (obj.hasAttr("attributes")) {
            Consumer_->OnBeginAttributes();
            ProcessItems(Py::Mapping(GetAttr(obj, "attributes")).items());
            Consumer_->OnEndAttributes();
        }
        // TODO(ignat): maybe use IsInstance in all cases?
        if (obj.isInteger()) {
            Consumer_->OnIntegerScalar(Py::Int(obj).asLongLong());
        } else if (obj.isFloat()) {
            Consumer_->OnDoubleScalar(Py::Float(obj));
        } else if (IsStringLike(obj)) {
            Consumer_->OnStringScalar(ConvertToStroka(ConvertToString(obj)));
        } else if (obj.isSequence()) {
            const auto& objList = Py::Sequence(obj);
            Consumer_->OnBeginList();
            for (auto it = objList.begin(); it != objList.end(); ++it) {
                Consumer_->OnListItem();
                Process(*it);
            }
            Consumer_->OnEndList();
        } else if (obj.isMapping()) {
            Consumer_->OnBeginMap();
            ProcessItems(Py::Mapping(obj).items());
            Consumer_->OnEndMap();
        } else {
            throw Py::RuntimeError(
                "Cannot convert this Python object to YSON: " +
                std::string(obj.repr()));
        }
    }

    // TODO(babenko): VisitItems? move to private?
    void ProcessItems(const Py::List& items)
    {
        // Unfortunately const_iterator doesn't work for mapping,
        // so we use iterator over items.
        for (auto it = items.begin(); it != items.end(); ++it) {
            const Py::Tuple& item(*it);
            const auto& key = item.getItem(0);
            const auto& value = item.getItem(1);
            if (!key.isString()) {
                throw Py::RuntimeError(
                    "Cannot use this Python object as dictionary in YSON: " + 
                    std::string(key.repr()));
            }
            Consumer_->OnKeyedItem(ConvertToStroka(ConvertToString(key)));
            Process(value);
        }
    }

private:
    IYsonConsumer* Consumer_;

};

} // anonymous namespace

void Serialize(const Py::Object& obj, IYsonConsumer* consumer)
{
    TPythonYTreeProducer(consumer).Process(obj);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
