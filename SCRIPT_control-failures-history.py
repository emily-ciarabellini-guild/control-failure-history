import os
from os.path import join, dirname
from dotenv import load_dotenv
import snowflake.connector


dotenv_path = join(dirname(__file__),'.env')
load_dotenv(dotenv_path)
 # Get the credentials from .env
SF_ACCOUNT    = os.getenv('SF_ACCOUNT')
SF_USER       = os.getenv('SF_USER')
SF_WAREHOUSE  = os.getenv('SF_WAREHOUSE')
SF_DATABASE   = os.getenv('SF_DATABASE')
SF_SCHEMA     = os.getenv('SF_SCHEMA')
SF_PASSWORD   = os.getenv('SF_PASSWORD')
SF_ROLE       = os.getenv('SF_ROLE')


connection = snowflake.connector.connect(
authenticator='externalbrowser',
user = SF_USER,
role = SF_ROLE, 
account  = SF_ACCOUNT,
warehouse = SF_WAREHOUSE,
database = SF_DATABASE,
password = SF_PASSWORD,
schema   = SF_SCHEMA)

# cursor_list = connection.execute_string(
#     "SELECT DISTINCT CONCAT(TERM_CODE,'_', PARTNER_STUDENT_ID) AS KEY, MAX(UPDATED_AT)     FROM GUILD.TA_ORCHESTRATOR_PUBLIC.STUDENT_TERM_LINE_ITEMS     WHERE CURRENT_STATE_NAME = 'Committed'   GROUP BY KEY LIMIT 10"
#     )

# for cursor in cursor_list:
#    for row in cursor:
#       print(row[0], row[1]) 

list = connection.execute_string(
    "SELECT DISTINCT CONCAT(TERM_CODE,'_', PARTNER_STUDENT_ID) AS KEY, MAX(UPDATED_AT)     FROM GUILD.TA_ORCHESTRATOR_PUBLIC.STUDENT_TERM_LINE_ITEMS     WHERE CURRENT_STATE_NAME = 'Committed'   GROUP BY KEY LIMIT 10"
    )

for x in list:
   for row in x:
      print(row[0], row[1]) 
