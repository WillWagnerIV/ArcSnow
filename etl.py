# -*- coding: utf-8 -*-

import os
import csv
import arcpy
import arcsnow as asn
import pandas as pd

class csv_upload(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Upload CSV"
        self.description = "Upload a CSV as a table to Snowflake"
        self.canRunInBackground = False
        self.category = "ETL"

    def _dtype_to_ftype(self, s):
        lookup = {
            "float":"DOUBLE",
            "float64":"DOUBLE",
            "int":"INT",
            "int64":"INT",
            "string":"VARCHAR",
            "object":"VARCHAR",
            "datetime":"DATETIME"            
        }

        return lookup[str(s)]

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
            
        input_csv_path = arcpy.Parameter(
            displayName="Input CSV",
            name="in_csv",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        table_name = arcpy.Parameter(
            displayName="Table Name",
            name="table_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        
        field_definitions = arcpy.Parameter(
            displayName="Field Definitions",
            name="field_definition",
            datatype="GPValueTable",
            parameterType="Optional",
            direction="Input")

        field_definitions.columns = [
            ['GPString', 'Name'],
            ['GPString', 'Type'],
            ['GPLong', 'Length'],
            ['GPBoolean', 'Nullable']
        ]
        
        field_definitions.filters[1].type = 'ValueList'
        field_definitions.filters[1].list = ['VARCHAR', 'DOUBLE', 'INT', 'DATETIME']
               
        out_table_name = arcpy.Parameter(
            displayName="Output Table Name",
            name="out_table_name",
            datatype="GPString",
            parameterType="Derived",
            direction="Output")
        
        params = [credentials, input_csv_path, table_name, field_definitions, out_table_name]
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
            
            df = pd.read_csv(parameters[1].valueAsText, engine='python', sep=',\s+', quoting=csv.QUOTE_ALL)
            fields = []
            for i in range(len(df.columns)):
                fields.append([
                    self._fix_field_name(df.columns[i]), # field name
                    self._dtype_to_ftype(df.dtypes[i]), # field type
                    255 if self._dtype_to_ftype(df.dtypes[i]) == "VARCHAR" else None, #field length
                    True, # nullable
                ])
            parameters[3].values = fields
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        arcsnow = asn.ArcSnow(parameters[0].valueAsText)
        arcsnow.login()

        table_name = parameters[2].valueAsText
        field_definitions = parameters[3].values
        field_names = [f[0] for f in field_definitions]
        arcsnow.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                        
        fields = []
        for field in field_definitions:
            field_name = field[0]
            field_type = field[1]
            if field_type == "VARCHAR":
                field_type += f'({field[2]})'

            fields.append(f'{field_name} {field_type}')
            
        fields = ",".join(fields)

        create_table = f"CREATE TABLE {table_name} ({fields});"
        arcpy.AddMessage(create_table)

        arcsnow.cursor.execute(create_table)
        arcsnow.cursor.execute(f'GRANT ALL ON {table_name} TO ROLE ACCOUNTADMIN;')
        arcsnow.cursor.execute(f'GRANT SELECT ON {table_name} TO ROLE PUBLIC;')
        
        df = pd.read_csv(parameters[1].valueAsText, engine='python', sep=',\s+', quoting=csv.QUOTE_ALL)

        fields = ",".join(field_names)
        rows = []
        for index, row in df.iterrows():
            data = []
            for index, c_name in enumerate(df):
                value = row[c_name]
                if field_definitions[index][1] == "VARCHAR":
                    data.append(f"'{value}'")
                else:
                    data.append(str(value))

            rows.append(",".join(data))

        values = ','.join([f'({x})' for x in rows])
        
        insert_values = f'INSERT INTO {table_name} ({fields}) VALUES {values};'
        
        arcpy.AddMessage(insert_values)

        arcsnow.cursor.execute(insert_values)
        arcsnow.logout()
        
        parameters[4].value = table_name
            
        return

