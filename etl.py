# -*- coding: utf-8 -*-

import os
import csv
import arcpy
from arcpy.arcobjects.arcobjects import Schema
import arcsnow as asn
import pandas as pd
import tempfile


# The Snowflake Connector library.
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas


df = pd.DataFrame()


class download_query(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Download Query"
        self.description = "Convert a Snowflake query to a GDB table"
        self.canRunInBackground = False
        self.category = "ETL"

    def getParameterInfo(self):
        """Define parameter definitions"""
                   
        credentials = arcpy.Parameter(
            displayName="Credentials File",
            name="credentials",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        sql_query = arcpy.Parameter(
            displayName="SQL Query",
            name="sql_query",
            datatype="GPSQLExpression",
            parameterType="Required",
            direction="Input")

        out_database = arcpy.Parameter(
            displayName="Target Database",
            name="out_database",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")

        out_database.value = arcpy.env.workspace

        out_name = arcpy.Parameter(
            displayName="Output Name",
            name="out_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
            
        out_table = arcpy.Parameter(
            displayName="Output Table",
            name="out_table",
            datatype="DETable",
            parameterType="Derived",
            direction="Output")
        
        return [credentials, sql_query, out_database, out_name, out_table]
    
    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        
        return

    def execute(self, parameters, messages):
        sql_query = parameters[1].valueAsText
        out_database = parameters[2].valueAsText
        out_name = parameters[3].valueAsText
        
        arcsnow = asn.ArcSnow(parameters[0].valueAsText)
        arcsnow.login()

        arcpy.AddMessage(sql_query)
        results = arcsnow.dict_cursor.execute(sql_query)
        first = results.fetchone()
        file_name = os.path.join(tempfile.gettempdir(), 'test.csv')
        
        with open(file_name, 'w', newline='') as csvfile:
            fields = list(first.keys())
            arcpy.AddMessage(fields)
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerow(first)
            
            for record in results:
                writer.writerow(record)
        
        arcpy.AddMessage("Converting CSV to database table")
        parameters[4].value = arcpy.conversion.TableToTable(file_name, out_database, out_name)
        

    def _field_type(self, field):
        if field[2] == 'TEXT' or field[2] == 'GEOGRAPHY':
            return 'TEXT'
        elif field[2] == 'NUMBER' and field[4] is not None:
            return 'DOUBLE'
        elif field[2] == "FLOAT":
            return 'FLOAT'
        elif field[2] == "BOOLEAN":
            return 'SHORT'
        else:
            return 'LONG'
            
    def _field_length(self, field):
        if field[2] == 'TEXT':
            return field[3]
        else:
            return 0
        
    def _field_name(self, field):
        return field[0]

    def _field_nullable(self, field):
        return 'NULLABLE' if field[1] == 'YES' else 'NON_NULLABLE'
        

class create_table(object):
    def __init__(self):
        self.label = "Create Table"
        self.description = "Convert a Snowflake table from a DETable"
        self.canRunInBackground = False
        self.category = "Snowflake"
        self._field_lookup = {
            "Double":"DOUBLE",
            "Single":"DOUBLE",
            "SmallInteger":"INT",
            "Integer":"INT",
            "String":"VARCHAR",
            "Date":"DATETIME",
            "Geometry":"GEOGRAPHY"
        }
        
    def getParameterInfo(self):
        credentials = arcpy.Parameter(
            displayName="Credentials File",
            name="credentials",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
            
        in_table = arcpy.Parameter(
            displayName="Input Table",
            name="in_table",
            datatype="DETable",
            parameterType="Required",
            direction="Input")

        out_name = arcpy.Parameter(
            displayName="Output Name",
            name="out_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
            
        out_table = arcpy.Parameter(
            displayName="Output Table",
            name="out_table",
            datatype="GPString",
            parameterType="Derived",
            direction="Output")
            
        return [credentials, in_table, out_name, out_table]
        
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
        
    def updateParameters(self, parameters):
        return
            
    def execute(self, parameters, messages):
        arcsnow = asn.ArcSnow(parameters[0].valueAsText)
        arcsnow.login()
        
        in_table = parameters[1].value
        table_name = parameters[2].valueAsText
        
        arcsnow.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        
        fields = [x for x in arcpy.ListFields(in_table) if x.type in self._field_lookup.keys()]
        
        sql_fields = ",".join([f"{x.name} {self._field_lookup[x.type]}" for x in fields])
        
        create_table = f"CREATE TABLE {table_name} ({sql_fields});"
        arcpy.AddMessage(create_table)

        arcsnow.cursor.execute(create_table)
        arcsnow.cursor.execute(f'GRANT ALL ON {table_name} TO ROLE ACCOUNTADMIN;')
        arcsnow.cursor.execute(f'GRANT SELECT ON {table_name} TO ROLE PUBLIC;')
        
        parameters[3].value = parameters[2].valueAsText
        
        return
    

class csv_upload(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Upload CSV"
        self.description = "Upload a CSV as a Table to Snowflake"
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

        return str(lookup[str(s)])

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
            
        input_csv = arcpy.Parameter(
            displayName="Input CSV",
            name="in_csv",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        db_name = arcpy.Parameter(
            displayName="Database Name",
            name="db_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        db_name.value = "ARCSNOW_DB"

        schema_name = arcpy.Parameter(
            displayName="Schema Name",
            name="schema_name",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")

        schema_name.value = "ARCSNOW_TESTING_SCHEMA"

        table_name = arcpy.Parameter(
            displayName="Table Name",
            name="table_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        

        # CSV Field Defs = params[4]
        csv_field_defs = arcpy.Parameter(
            displayName="Col Names to Field Definitions",
            name="field_definition",
            datatype="GPValueTable",
            parameterType="Optional",
            direction="Input")


        # Create a Table for Each Column

        # Field Defs Parameter Columns
        csv_field_defs.columns = [
            ['GPString', 'Name'],
            ['GPString', 'Type'],
            ['GPLong', 'Length'],
            ['GPBoolean', 'Nullable']
        ]
        
        # Field Defs Parameter Columns Filters
        csv_field_defs.filters[1].type = 'ValueList'
        csv_field_defs.filters[1].list = ['VARCHAR', 'DOUBLE', 'INT', 'DATETIME']


        # Output Table Name
        out_table_name = arcpy.Parameter(
            displayName="Output Table Name",
            name="out_table_name",
            datatype="GPString",
            parameterType="Derived",
            direction="Output")
        


        params = [credentials, input_csv, db_name, schema_name, table_name, csv_field_defs, out_table_name]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        if not parameters[1].hasBeenValidated and parameters[1].valueAsText:
            parameters[4].value = os.path.splitext(os.path.basename(parameters[1].valueAsText))[0]
            
            df = pd.read_csv(parameters[1].valueAsText, engine='python', sep=',\s+', quoting=csv.QUOTE_ALL)
            fields = []

            # Create a list of the df column names
            dcl = df.columns.copy(deep=True)

            arcpy.AddMessage(f"csv_field_defs: {parameters[5]}")

            for dc in dcl:

                # Create a row in the table for each Col/Field

                a_field = []
                print (f"dc info = {df.dtypes[dc]}")
                a_field_name = self._fix_field_name(dc)
                a_field_type = self._dtype_to_ftype(df.dtypes[dc]) # field type
                if a_field_type == "VARCHAR": 
                    a_field_len = 255
                else:
                    a_field_len = None
                a_field_nullable = True

                a_field = [a_field_name, a_field_type, a_field_len, a_field_nullable]

                print(f"Fields during Update: {a_field}")

                fields.append(a_field)
                parameters[5].values = fields

                # parameters[5].values.addRow(a_field)

        try:
            arcpy.AddMessage(f"csv_field_defs: {parameters[5].values}")
        except:
            pass
        try:
            arcpy.AddMessage(f"Update Fields: {fields}")
        except:
            pass
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""

        return

    def execute(self, parameters, messages):
        """Executes when Run button is pressed."""

        # Get the Credentials
        arcsnow = asn.ArcSnow(parameters[0].valueAsText)
        arcsnow.login()

        # Create a Cursor
        snow_cur = arcsnow.cursor

        # DB Info
        db_name = parameters[2].valueAsText
        schema_name = parameters[3].valueAsText
        table_name = parameters[4].valueAsText
        field_definitions = parameters[5].values
        field_names = [f[0] for f in field_definitions]
        
        arcpy.AddMessage(f"field_definitions: {parameters[5].values}")

        # Create a Schema and/or Drop the Table if it exists
        snow_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        snow_cur.execute(f"USE SCHEMA {schema_name};")
        snow_cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        # SAMPLE
        # CREATE TABLE "ARCSNOW_DB"."ARCSNOW_TESTING_SCHEMA"."SQL_TESTING_TABLE" ("C1" STRING);

        # Create an Explicit Table Name
        long_table_name = f'"{db_name}"."{schema_name}"."{table_name}"'

        # Create the SQL Statement
        create_table = f'CREATE TABLE IF NOT EXISTS {long_table_name} ('

        fields = []
        for field in field_definitions:
            field_name = field[0]
            create_table += f'"{field_name}" '
            field_type = field[1]
            if field_type == "VARCHAR":
                field_type += f'({field[2]})'

            create_table += f'{field_type}'

            fields.append(f'{field_name} {field_type} {field[3]}')
            
        fields = ",".join(fields)
        arcpy.AddMessage(f"Execute Fields: {fields}")

        create_table += f');'
        arcpy.AddMessage(f"Create Table SQL: {create_table}")

        snow_cur.execute(create_table)
        snow_cur.execute(f'GRANT ALL ON {long_table_name} TO ROLE ACCOUNTADMIN;')
        snow_cur.execute(f'GRANT SELECT ON {long_table_name} TO ROLE PUBLIC;')
        
        parameters[5].value = long_table_name

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
        
        insert_values = f'INSERT INTO {long_table_name} ({fields}) VALUES {values};'
        
        arcpy.AddMessage(insert_values)

        snow_cur.execute(insert_values)
        arcsnow.logout()
        
        parameters[6].values = long_table_name
            
        return



if __name__ == "__main__":
    asnow = asn.ArcSnow("CredentialsFile.ini")
    asnow.login()