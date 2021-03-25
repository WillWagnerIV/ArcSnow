import arcpy

from credentials import Credentials
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
            warehouse=self._credentials.warehouse,
            database=self._credentials.database,
            schema=self._credentials.schema
            )
        
        arcpy.AddMessage("Connection successful")
        
        self._conn.cursor().execute("USE ROLE ACCOUNTADMIN;")       
        self._conn.cursor().execute(f"USE WAREHOUSE {self._credentials.warehouse};")
        self._conn.cursor().execute(f"USE SCHEMA  {self._credentials.schema};")
        self._conn.cursor().execute(f"USE DATABASE {self._credentials.database}")
        
        arcpy.AddMessage("\n")
        arcpy.AddMessage("Current configuration")
        arcpy.AddMessage(f"  Role: ACCOUNTADMIN")
        arcpy.AddMessage(f"  Warehouse: {self._credentials.warehouse}")
        arcpy.AddMessage(f"  Database: {self._credentials.database}")
        arcpy.AddMessage(f"  Schema: {self._credentials.schema}")
    
    def logout(self):
        self._conn.cursor().close()
        self._conn.close()
        
    def schema(self, table_name):
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
        
if __name__ == "__main__":
    arcsnow = ArcSnow("CredentialsFile.ini")
    arcsnow.login()
