from pyspark.sql.types import UserDefinedType, BinaryType, LongType, StringType
from pyspark.sql.functions import udf


class YsonType(UserDefinedType):
    @classmethod
    def typeName(cls):
        return "yson"

    @classmethod
    def sqlType(cls):
        return BinaryType()

    @classmethod
    def module(cls):
        return 'spyt.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.yson.YsonType'

    def needConversion(self):
        return True

    def serialize(self, obj):
        return obj

    def deserialize(self, datum):
        return datum

    def simpleString(self):
        return 'yson'


class UInt64Type(UserDefinedType):
    @classmethod
    def typeName(cls):
        return "uint64"

    @classmethod
    def sqlType(cls):
        return LongType()

    @classmethod
    def module(cls):
        return 'spyt.types'

    @classmethod
    def scalaUDT(cls):
        return 'org.apache.spark.sql.yson.UInt64Type'

    def needConversion(self):
        return True

    def serialize(self, obj):
        return obj

    def deserialize(self, datum):
        return datum

    def simpleString(self):
        return 'uint64'


def uint64_to_string(number):
    if number is None:
        return None
    else:
        # convert to unsigned value
        return str(number & 0xffffffffffffffff)


def string_to_uint64(number):
    if number is None:
        return None
    else:
        return int(number)


uint64_to_string_udf = udf(uint64_to_string, StringType())
string_to_uint64_udf = udf(string_to_uint64, UInt64Type())
