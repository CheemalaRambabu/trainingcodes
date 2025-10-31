package com.cognizant.kafkasparkstreaming;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Utility {
        
        public static StructType employeeSchema() {
                StructField idField = DataTypes.createStructField("id", DataTypes.IntegerType, false);
                StructField nameField = DataTypes.createStructField("name", DataTypes.StringType, false);
                StructField designationField = DataTypes.createStructField("designation", DataTypes.StringType, false);
                StructType empSchema=new StructType(new StructField[] {idField,nameField,designationField});
                return empSchema;
        }

}
