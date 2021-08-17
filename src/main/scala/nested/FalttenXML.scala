package com.spglobal
package nested

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.xml
import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FalttenXML {

  def expand_nested_column(xml_data_df_temp: DataFrame): DataFrame = {
    var xml_data_df: DataFrame = xml_data_df_temp
    var select_clause_list = List.empty[String]

    for(column_name <- xml_data_df.schema.names){
      println("Outside isInstance loop: "+ column_name)

      if(xml_data_df.schema(column_name).dataType.isInstanceOf[ArrayType]){
        println("Inside isInstance loop: "+ column_name)

        xml_data_df = xml_data_df.withColumn(column_name, explode(xml_data_df(column_name)).alias(column_name))
        select_clause_list :+= column_name
      }

      else if(xml_data_df.schema(column_name).dataType.isInstanceOf[StructType]){
        println("Inside isInstance loop: "+column_name)

        for(field <- xml_data_df.schema(column_name).dataType.asInstanceOf[StructType].fields){
          select_clause_list :+= column_name + "." + field.name
        }
      }

      else{
        select_clause_list :+= column_name
      }
    }
    val columnNames = select_clause_list.map(name => col(name).alias(name.replace('.','@')))
    xml_data_df.select(columnNames:_*)
  }

  //to extract the table name to which the current flattened dataframe column belongs to
  def extract_table_name(column:String):String={
    var sub_string:String = ""
    if(column.indexOf('@') == -1){
      sub_string = column
    }
    else {
      sub_string = column.substring(0,column.lastIndexOf('@'))
      sub_string = sub_string.substring(0,sub_string.lastIndexOf('@'))
      sub_string = sub_string.substring(sub_string.lastIndexOf('@')+1)
    }

    //println(sub_string)
    return sub_string
  }

  //To extract the column name for the individual table from the flattened dataframe.
  def extract_column_name(column:String):String={
    var sub_string:String = ""
    if(column.indexOf('@') == -1){
      sub_string = column
    }
    else {
      sub_string = column.substring(column.lastIndexOf('@')+1)
    }

    //println(sub_string)
    return sub_string
  }

  def main(args: Array[String]): Unit={
    println("Spark Application started")

    val spark = SparkSession.builder().appName("Create dataframe from nested XML").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val xml_file_path = "D:\\xmlfile.xml"
    var xml_data_df = spark.read.option("rowtag","Instrument_Root").xml(xml_file_path)

    xml_data_df.show()
    xml_data_df.printSchema()

    var nested_column_count = 1

    while(nested_column_count!=0){
      println("Printing nested column count: "+nested_column_count)

      var nested_column_count_temp = 0

      for(column_name <- xml_data_df.schema.names){
        println("Iterating dataframe column: "+column_name)

        if(xml_data_df.schema(column_name).dataType.isInstanceOf[ArrayType] ||
          xml_data_df.schema(column_name).dataType.isInstanceOf[StructType]){
          nested_column_count_temp+=1
        }

        if(nested_column_count_temp!=0){
          xml_data_df = expand_nested_column(xml_data_df)
          xml_data_df.show()
        }

        println("Printing nested_column_count_temp: "+nested_column_count_temp)
        nested_column_count = nested_column_count_temp
      }
    }

    //Extract columns into separate dataframes.

    var dataframe_map = new scala.collection.mutable.HashMap[String,DataFrame]
    var table_column_map = new scala.collection.mutable.HashMap[String,ListBuffer[String]]

    val column_list = xml_data_df.columns.toList
    var table_name:String =""
    var empty_data_frame = spark.emptyDataFrame

    //Creating the keys in the maps with empty values to be populated later

    for(column <- column_list){
      table_name = extract_table_name(column)
      if(table_name.equalsIgnoreCase(column))
        table_name = "Instrument_Roots"
      dataframe_map+= (table_name -> empty_data_frame)
      table_column_map+= (table_name -> ListBuffer.empty[String])
    }

    //Populate each table with its relevant column names list
    for(column <- column_list){
      table_name = extract_table_name(column)
      if(table_name.equalsIgnoreCase(column))
        table_name = "Instrument_Roots"
      table_column_map.get(table_name).get += column
    }

    println(table_column_map.keys)
    println(table_column_map.values)

    //Select relevant columns into individual dataframes
    for(table <- dataframe_map.keySet){
      var columnNames = table_column_map(table)
      println("Table name: "+table)
      println("Column list: "+columnNames)
      dataframe_map(table) = xml_data_df.select(columnNames.map(m=>col(m).alias(extract_column_name(m))):_*).distinct()
    }

    dataframe_map("Instrument_Rating_Attributes").show()//Sample print statement
  }
}
