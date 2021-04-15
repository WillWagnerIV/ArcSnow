# -*- coding: utf-8 -*-

import os
import csv
import arcpy
import arcsnow as asn
import pandas as pd

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
        file_name = 'test.csv'
        
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

