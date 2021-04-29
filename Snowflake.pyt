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
        self.alias = "snowflake"

        # List of tool classes associated with this toolbox
        self.tools = [
            test_credentials, 
            create_table, 
            csv_upload, 
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
            name="in_layer",
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

