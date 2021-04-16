import os
import sys
import csv
import arcpy
import arcsnow as asn

class update_comment(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Update Column Comments"
        self.description = "Update columna comments from CSV exported from Dataedo"
        self.canRunInBackground = False
        self.category = "Dataedo"
    
    def getParameterInfo(self):
        """Define parameter definitions"""
        credentials = arcpy.Parameter(
            displayName="Credentials File",
            name="credentials",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
            
        in_table = arcpy.Parameter(
            displayName="Input CSV",
            name="in_table",
            datatype="DETable",
            parameterType="Required",
            direction="Input")
            
        success = arcpy.Parameter(
            displayName="Success",
            name="valid",
            datatype="GPBoolean",
            parameterType="Derived",
            direction="Output")
            
        return [credentials, in_table, success]
        
    def updateParameters(self, parameters):
        return
    
    def execute(self, parameters, messages):
        arcsnow = asn.ArcSnow(parameters[0].valueAsText)
        arcsnow.login()
        
        csv_file_path = parameters[1].valueAsText
        with open(csv_file_path, "r") as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)
            
            table_index = 1
            column_index = 5
            comment_index = 15
            
            for row in csv_reader:
                table_name = row[table_index]
                column_name = row[column_index]
                comment = row[comment_index]
                
                sql = f"COMMENT ON COLUMN {table_name}.{column_name} IS '{comment}';"
                arcsnow.cursor.execute(sql)
