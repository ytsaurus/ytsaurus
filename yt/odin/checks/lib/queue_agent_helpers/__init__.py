from yt import yson


BANNED_ATTRIBUTE_NAME = "banned"
MAINTENANCE_ATTRIBUTE_NAME = "maintenance"


def is_attribute_true(yson_instance, attribute_name) -> bool:
    if yson_instance.has_attributes() and attribute_name in yson_instance.attributes:
        attribute_value = yson_instance.attributes[attribute_name]
        # NB(apachee): Check type to match the behavior of queue agent sharding manager
        # (it ignores anything except bool).
        return isinstance(attribute_value, yson.YsonBoolean) and attribute_value

    return False
