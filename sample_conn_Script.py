import snowflake.connector
from snowflake.connector import DictCursor
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import traceback
import socket

password = 'rohit'.encode()
print(password)

with open('C:/Users/rkuma162/rsa_key.p8') as f:
	lines = f.read()

print(lines)
f.close()

with open("C:/Users/rkuma162/rsa_key.p8", "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=password,
        backend=default_backend()
    )
	
print(p_key)	

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())	

print(pkb)	
conn_config = {
"account": "kgasizd-zm63439",
"user": "rkumar_kvp",
"private_key": pkb,
"role": "ACCOUNTADMIN",
"warehouse": "COMPUTE_WH",
"database": "EXERCISE_DB",
"schema": "FIRST_SCHEMA"
}  

conn = snowflake.connector.connect(**conn_config)
cur = conn.cursor()

try:
	cur.execute("SELECT COUNT(*) FROM EXERCISE_DB.FIRST_SCHEMA.EMPLOYEE;")
	#cur.execute("SELECT current_version()")
	result = cur.fetchone()
	print("*********")
	print(result[0])
finally:
	cur.close()
conn.close()


