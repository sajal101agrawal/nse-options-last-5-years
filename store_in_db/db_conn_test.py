import os, psycopg2, dotenv; dotenv.load_dotenv()
print("Connecting ...")
conn = psycopg2.connect(os.getenv("PG_DSN") or (
    "dbname={db} user={user} password={pwd} host={host} port={port} sslmode=require"
    ).format(db=os.getenv("PG_DB"), user=os.getenv("PG_USER"),
             pwd=os.getenv("PG_PASSWORD"), host=os.getenv("PG_HOST"),
             port=os.getenv("PG_PORT")))
print("OK!"); conn.close()