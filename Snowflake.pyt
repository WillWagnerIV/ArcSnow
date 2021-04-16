# -*- coding: utf-8 -*-

import os
import sys

import arcpy
import csv
import pandas as pd

from arcsnow import test_credentials
from credentials import generate_credentials
from etl import csv_upload
from etl import create_table
from etl import download_query
from update_column_comment import update_comment

import credentials
import arcsnow as asn


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Snowflake ETL"
        self.alias = "snowflake_toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [
            test_credentials, 
            create_table, 
            csv_upload, 
            feature_class_upload, 
            download_query, 
            generate_credentials,
            insert_into,
            update_comment
        ]
        
class insert_into(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Insert Into"
        self.description = "Insert rows into a table inside of Snowflake"
        self.canRunInBackground = False
        self.category = "Snowflake"
    
    def getParameterInfo(self):
        """Define parameter definitions"""
        credentials = arcpy.Parameter(
            displayName="Credentials File",
            name="credentials",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
            
        in_table = arcpy.Parameter(
            displayName="Input Feature Layer",
            name="in_table",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input")
            
        target_table = arcpy.Parameter(
            displayName="Target Table Name",
            name="target_table_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        
        out_table_name = arcpy.Parameter(
            displayName="Output Table Name",
            name="out_table_name",
            datatype="GPString",
            parameterType="Derived",
            direction="Output")
            
        return [credentials, in_table, target_table, out_table_name]
            
    def updateParameters(self, parameters):
        return
        
    def _flush_batch(self, cursor, data, table_name):
        
        arcpy.AddMessage(data[0])
        data = ','.join([f'({x})' for x in data])
        
        
        insert_values = f'INSERT INTO {table_name} VALUES {data};'
        cursor.execute(insert_values)
        pass
        
    def execute(self, parameters, messages):
        arcsnow = asn.ArcSnow(parameters[0].valueAsText)
        arcsnow.login()
        
        in_table = parameters[1].value
        table_name = parameters[2].valueAsText
        
        fields = [x for x in arcpy.ListFields(in_table) if not x.name == arcpy.Describe(in_table).OIDFieldName]
        
        arcpy.AddMessage([x.name if not x.type == 'Geometry' else "SHAPE@" for x in fields])
        
        with arcpy.da.SearchCursor(in_table, [x.name if not x.type == 'Geometry' else "SHAPE@" for x in fields]) as SC:
            max_batch = 1000
            values = []
            
            for row in SC:
                data = []
                for index, col in enumerate(row):
                    if col == None:
                        data.append("NULL")
                    elif fields[index].type == 'Geometry':
                        data.append(f"'{col.WKT}'")
                    elif fields[index].type == 'String' or fields[index].type == 'Date':
                        data.append(f"'{col}'")
                    else:
                        data.append(str(col))
                    
                values.append(",".join(data))
                
                if len(values) == max_batch:
                    self._flush_batch(arcsnow.cursor, values, table_name)
                    values = []
                    
            
            if len(values):
               self._flush_batch(arcsnow.cursor, values, table_name)
        
        parameters[3].value = parameters[2].valueAsText
         
class feature_class_upload(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Upload Feature Class"
        self.description = "Upload a Feature Class as a table to Snowflake"
        self.canRunInBackground = False
        self.category = "ETL"
        self._field_lookup = {
            "Double":"DOUBLE",
            "Single":"DOUBLE",
            "SmallInteger":"INT",
            "String":"VARCHAR",
            "Date":"DATETIME",
            "Geometry":"GEOGRAPHY"
        }

    def _fix_field_name(self, s):
        s = s.strip()
        
        for c in "()+~`-;:'><?/\\|\" ^":
            s = s.replace(c, "_")

        while "__" in s:
            s = s.replace("__", "_")

        if s[0] == "_":
            s = s[1:]

        if not s[0].isalpha():
            s = "t" + s

        if s[-1] == "_":
            s = s[:-1]
            
        return s

    def getParameterInfo(self):
        """Define parameter definitions"""
        credentials = arcpy.Parameter(
            displayName="Credentials File",
            name="credentials",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        in_fl = arcpy.Parameter(
            displayName="Input Feature Layer",
            name="in_csv",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input")
        
        table_name = arcpy.Parameter(
            displayName="Table Name",
            name="table_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
            
        out_table_name = arcpy.Parameter(
            displayName="Output Table Name",
            name="out_table_name",
            datatype="GPString",
            parameterType="Derived",
            direction="Output")
        
        params = [credentials, in_fl, table_name, out_table_name]
        return params
    
    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        if not parameters[1].hasBeenValidated and parameters[1].valueAsText:
            parameters[2].value = os.path.splitext(os.path.basename(parameters[1].valueAsText))[0]
            parameters[2].value = parameters[2].value.replace(" ", "_")
            
        return


    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        arcsnow = asn.ArcSnow(parameters[0].valueAsText)
        arcsnow.login()

        in_fc = parameters[1].value
        table_name = parameters[2].valueAsText
        
        arcsnow.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        fields = [x for x in arcpy.ListFields(in_fc) if x.type in self._field_lookup.keys()]
        
        sql_fields = ",".join([f"{x.name} {self._field_lookup[x.type]}" for x in fields])
        
        create_table = f"CREATE TABLE {table_name} ({sql_fields});"
        arcpy.AddMessage(create_table)

        arcsnow.cursor.execute(create_table)
        arcsnow.cursor.execute(f'GRANT ALL ON {table_name} TO ROLE ACCOUNTADMIN;')
        arcsnow.cursor.execute(f'GRANT SELECT ON {table_name} TO ROLE PUBLIC;')
        
        field_names = [f.name for f in fields if not f.type == "Geometry"] + ["SHAPE@"]
        arcpy.AddMessage(field_names)
        
        rows = []
        with arcpy.da.SearchCursor(in_fc, field_names) as SC:
            for row in SC:
                arcpy.AddMessage(row)
        # for index, row in df.iterrows():
            # data = []
            # for index, c_name in enumerate(df):
                # value = row[c_name]
                # if field_definitions[index][1] == "VARCHAR":
                    # data.append(f"'{value}'")
                # else:
                    # data.append(str(value))

            # rows.append(",".join(data))

        # values = ','.join([f'({x})' for x in rows])
        
        # insert_values = f'INSERT INTO {table_name} ({fields}) VALUES {values};'
        
        # arcpy.AddMessage(insert_values)

        # arcsnow.cursor.execute(insert_values)
        arcsnow.logout()
        
        parameters[3].value = table_name
        
        return

