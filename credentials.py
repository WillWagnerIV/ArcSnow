import sys
import os
import ctypes
import arcpy

from cryptography.fernet import Fernet

class Credentials(object):
    def __init__(self, path=None):
        self.__key_file = 'key.key'
        self.__key = ""
        self.__cred_file = "CredentialsFile.ini"
        self.location = "./"
        
        if path and self.__read_from_path(path):
            return
                
        #Stored within the Credential File
        self.username = ""
        self.__password = ""
        self.account = ""
        self.warehouse = ""
        self.database = ""
        self.schema = ""
        #File names and decyrption key


    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, password):
        self.__key = Fernet.generate_key()
        f = Fernet(self.__key)
        self.__password = f.encrypt(password.encode()).decode()
        del f
        
    @property
    def rawpass(self):
        f = Fernet(self.__key)
        decrypted = f.decrypt(self.__password.encode()).decode()
        del f
        
        return decrypted
        
    @property
    def path(self):
        return os.path.join(self.location, self.__cred_file)

    def create_cred(self):
        cred_filename = os.path.join(self.location, self.__cred_file)
        key_filename = os.path.join(self.location, self.__key_file)

        with open(cred_filename, 'w') as file_in:
            file_in.write(f"#Credential File:\nUsername={self.username}\nPassword={self.__password}\nAccount={self.account}\nWarehouse={self.warehouse}\nDatabase={self.database}\nSchema={self.schema}")

        if(os.path.exists(key_filename)):
            os.remove(key_filename)

        try:
            os_type = sys.platform
            with open(key_filename, 'w') as key_in:
                key_in.write(self.__key.decode())
                if(os_type == 'win32'):
                    ctypes.windll.kernel32.SetFileAttributesW(self.__key_file, 2)

        except PermissionError:
            os.remove(key_filename)
            arcpy.AddMessage("A Permission Error occurred.\n Please re run the script")

    def __read_from_path(self, path):
        try:
            cred_filename = path

            #The key file to decrypt password
            key_file = os.path.join(os.path.dirname(cred_filename), self.__key_file) 

            self.__key = ''
            with open(key_file, 'r') as key_in:
                    self.__key = key_in.read().encode()

            #Sets the value of decrpytion key    
            f = Fernet(self.__key)

            #Loops through each line of file to populate a dictionary from tuples in the form of key=value
            with open(cred_filename, 'r') as cred_in:
                lines = cred_in.readlines()
                config = {}
                for line in lines:
                    tuples = line.rstrip('\n').split('=',1)
                    if tuples[0] in ('Username', 'Password', 'Account', 'Warehouse', 'Database', 'Schema'):
                        config[tuples[0]] = tuples[1]

                #Password decryption
                passwd = f.decrypt(config['Password'].encode()).decode()
                config['Password'] = passwd
                
                self.username = config['Username']
                self.password = config['Password']
                self.account = config['Account']
                self.warehouse = config['Warehouse']
                self.database = config['Database']
                self.schema = config['Schema']
            return True
            
        except:
            return False
            

class test_credentials(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Test Credentials"
        self.description = "Test a Snowflake credential"
        self.canRunInBackground = False
        self.category = "Preparation"
        
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
        
        arcpy.AddMessage("Here")
        arcsnow = ArcSnow(parameters[0].valueAsText)
        arcpy.AddMessage("Here")
        
        arcsnow.login()
        arcpy.AddMessage("Here")
        
        parameters[1].value = True
        
        
class generate_credentials(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Generate Credential File"
        self.description = "Create a credentials file used to authenticate with Snowflake"
        self.canRunInBackground = False
        self.category = "Preparation"
        
    def getParameterInfo(self):
        """Define parameter definitions"""
        username = arcpy.Parameter(
            displayName="Username",
            name="username",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
            
        password = arcpy.Parameter(
            displayName="Password",
            name="password",
            datatype="GPStringHidden",
            parameterType="Required",
            direction="Input")
            
        account = arcpy.Parameter(
            displayName="Account",
            name="account",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
            
        warehouse = arcpy.Parameter(
            displayName="Warehouse",
            name="warehouse",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
            
        database = arcpy.Parameter(
            displayName="Database",
            name="database",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
            
        schema = arcpy.Parameter(
            displayName="Schema",
            name="schema",
            datatype="GPString",
            parameterType="Required",
            direction="Input")

        output_path = arcpy.Parameter(
            displayName="Output Location",
            name="output",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")
            
        out_file = arcpy.Parameter(
            displayName = "Out Credential File",
            name = "credential_filepath",
            datatype = "DEFile",
            parameterType = "Derived",
            direction = "Output")
         
        output_path.value = arcpy.mp.ArcGISProject("CURRENT").homeFolder

        return [username, password, account, warehouse, database, schema, output_path, out_file]

    def updateParameters(self, parameters):
        if not parameters[6].value:
            parameters[6].value = arcpy.mp.ArcGISProject("CURRENT").homeFolder
                      
        if not parameters[6].hasBeenValidated or not parameters[6].altered:
            credentials = Credentials()
            credentials.location = parameters[6].valueAsText
            parameters[7].value = credentials.path
        
    def execute(self, parameters, messages):        
        credentials = Credentials()
        
        credentials.username = parameters[0].valueAsText
        credentials.password = parameters[1].valueAsText
        credentials.account = parameters[2].valueAsText
        credentials.warehouse = parameters[3].valueAsText
        credentials.database = parameters[4].valueAsText
        credentials.schema = parameters[5].valueAsText
        credentials.location = parameters[6].valueAsText
        
        credentials.create_cred()
        
        parameters[7].value = credentials.path
        
        
if __name__ == "__main__":
    credentials = Credentials()
    credentials.username = "HYIMHK"
    credentials.password = "test"
    credentials.account = "ppa07348"
    credentials.warehouse = "COMPUTE_WH"
    credentials.database = "DEMO_DB"
    credentials.schema = "PUBLIC"
    
    credentials.create_cred()
