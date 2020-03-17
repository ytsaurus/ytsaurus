package ru.yandex.spark.example;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class UdfToPythonExample {
    public static UserDefinedFunction contains = functions.udf((String s, String sub) -> s.contains(sub), DataTypes.BooleanType);

    public static UserDefinedFunction parseString = functions.udf((UDF1<String, Integer>) s -> {
        String[] split = s.split(",");
        if (split.length > 0) {
            return split.length;
        } else {
            return 1;
        }
    }, DataTypes.IntegerType);
}
