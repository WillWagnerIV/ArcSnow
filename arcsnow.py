import arcpy

from credentials import Credentials
from snowflake.connector import DictCursor
import snowflake.connector

class ArcSnow(object):
    def __init__(self, path):
        self._credentials = Credentials(path)
        self._conn = None
        
    def login(self):
    
        self._conn = snowflake.connector.connect(
            user=self._credentials.username,
            password=self._credentials.rawpass,
            account=self._credentials.account,
            role=self._credentials.role,
            warehouse=self._credentials.warehouse,
            database=self._credentials.database,
            db_schema=self._credentials.db_schema
            )
        
        arcpy.AddMessage("Connection successful")
        
        
        arcpy.AddMessage("\n")
        arcpy.AddMessage("Current configuration")
        self._conn.cursor().execute(f"USE ROLE {self._credentials.role};")       
        arcpy.AddMessage(f"  Role: {self._credentials.role}")
        self._conn.cursor().execute(f"USE WAREHOUSE {self._credentials.warehouse};")
        arcpy.AddMessage(f"  Warehouse: {self._credentials.warehouse}")
        self._conn.cursor().execute(f"USE DATABASE {self._credentials.database}")
        arcpy.AddMessage(f"  Database: {self._credentials.database}")
        self._conn.cursor().execute(f"USE SCHEMA  {self._credentials.db_schema};")
        arcpy.AddMessage(f"  Schema: {self._credentials.db_schema}")
    
    def logout(self):
        self._conn.cursor().close()
        self._conn.close()
        
    def get_schema(self, table_name):
        results = self._conn.cursor().execute("""SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION \
        FROM INFORMATION_SCHEMA.COLUMNS\
        WHERE TABLE_NAME='{}'""".format(table_name))
        for r in results:
            print(r)
        
    @property
    def conn(self):
        return self._conn
    
    @property
    def cursor(self):
        return self._conn.cursor()
        
    @property
    def dict_cursor(self):
        return self._conn.cursor(snowflake.connector.DictCursor)
        

class test_credentials(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Test Credentials"
        self.description = "Test ArcSnow (Snowflake) credentials and defaults."
        self.canRunInBackground = False
        self.category = "Pre-Flight"
        
    def getParameterInfo(self):
        """Define parameter definitions"""
        credentials = arcpy.Parameter(
            displayName="Credentials File",
            name="credentials",
            datatype="DEFile",
            parameterType="Required",
            direction="Input")
        
        valid = arcpy.Parameter(
            displayName="Is Valid",
            name="valid",
            datatype="GPBoolean",
            parameterType="Derived",
            direction="Output")
        
        return [credentials, valid]
        
    def execute(self, parameters, messages):
        parameters[1].value = False
        
        arcsnow = ArcSnow(parameters[0].valueAsText)
        arcsnow.login()
        
        arcpy.AddMessage(f"Schema: {arcsnow._credentials.db_schema}")

        parameters[1].value = True

if __name__ == "__main__":
    arcsnow = ArcSnow("CredentialsFile.ini")
    arcsnow.login()

