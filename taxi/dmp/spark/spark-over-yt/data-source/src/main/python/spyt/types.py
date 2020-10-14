from pyspark.sql.types import UserDefinedType, BinaryType


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
