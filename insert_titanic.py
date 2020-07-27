import pandas as pd 
import psycopg2
from psycopg2 import extras

df = pd.read_csv('https://raw.githubusercontent.com/LambdaSchool/DS-Unit-3-Sprint-2-SQL-and-Databases/master/module2-sql-for-analysis/titanic.csv')
print(df.head())

connection = psycopg2.connect(dbname="pfcxenqo",
                              user="pfcxenqo",
                              password="fUj3ZYls0t-xpgB_zQEZcaIHTxDGfxZJ",
                              host="rogue.db.elephantsql.com")

curseur = connection.cursor()

query0 = """
         DROP TYPE IF EXISTS GENDER;
         """

curseur.execute(query0)

query1 = """
         CREATE TYPE Gender AS ENUM ('male', 'female');
         """
curseur.execute(query1)

query2 = """
         CREATE TABLE IF NOT EXISTS Titanic (
                id  SERIAL PRIMARY KEY,
                Survived int4,
                Pclass int4,
                name  varchar(100) NOT NULL,
                Gender Gender,
                Age  int4,
                Siblings_Spouses_Aboard  int4,
                Parents_Children_Aboard  int4,
                Fare  float8);
         """

curseur.execute(query2)

df = df.astype("object")
df_tuples = list(df.to_records(index=False))

query3 = """
         INSERT INTO Titanic 
         (Survived, Pclass, name, Gender, Age, Siblings_Spouses_Aboard, Parents_Children_Aboard, Fare)
         VALUES %s
         """

extras.execute_values(curseur, query3, df_tuples)
connection.commit()
curseur.close()
connection.close()
