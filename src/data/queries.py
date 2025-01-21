import psycopg2
from config import config

# Create connect function and test the connection
def connect():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM person;'
        cursor.execute(SQL)
        row = cursor.fetchone()
        print("Testing connection:")
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Query for all the rows in the person table and print them
def all_rows():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM person;'
        cursor.execute(SQL)
        row = cursor.fetchall()
        print("Querying all rows from person:")
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Query for the column names in the person table and print them
def column_names():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * from person;'
        cursor.execute(SQL)
        column_names = [desc[0] for desc in cursor.description]
        print("Column names in person:")
        print(column_names)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()    

# Query for the certificate table column names as well as the rows and print them
def cert_names_rows():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * from certificates;'
        cursor.execute(SQL)
        column_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        print("Column names and rows in certificates:")
        print(column_names)
        print(rows)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Query person table for average age and print it
def avg_age():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT AVG(age) from person;'
        cursor.execute(SQL)
        avgage = cursor.fetchone()
        print("Average age in person table:")
        print(avgage)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Try another query which you learned before
def person_scrum():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT p.id, p.name, age, student FROM person p INNER JOIN certificates c ON p.id = c.person_id WHERE c.name = %s;' # %s placeholder to pass 'Scrum' as an argument
        cursor.execute(SQL, ('Scrum',))
        rows = cursor.fetchall()
        print("All persons with Scrum certificate:")
        print(rows)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Add a new row to the certificate table in a way that the inserted values are taken as function parameters
def db_create_cert(name: str, person_id: int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'INSERT INTO certificates (name, person_id) VALUES (%s, %s);'
        data = name, person_id
        cursor.execute(SQL, data) # Pass argument to executor
        con.commit() # Save the changes to database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Add a new row to the person table by entering values afterwards. Values are taken as function parameters
def db_create_person(name: str, age: int, student: bool):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'INSERT INTO person (name, age, student) VALUES (%s, %s, %s);'
        data = name, age, student
        cursor.execute(SQL, data) # Pass argument to executor
        con.commit() # Save the changes to database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Testing that inserting person worked
def test_person_function():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM person;'
        cursor.execute(SQL)
        rows = cursor.fetchall()
        print("Testing if inserting worked:")
        print(rows)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Testing that inserting certificate worked
def test_cert_function():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM certificates;'
        cursor.execute(SQL)
        rows = cursor.fetchall()
        print("Testing if inserting worked:")
        print(rows)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# Update an existing row in the person table
def upd_person(name: str):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'UPDATE person SET name = %s WHERE id=10;'
        data = name
        cursor.execute(SQL, (data,)) # Pass argument to executor
        con.commit() # Save the changes to database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()    

# Update an existing row in the certificate table
def upd_cert(person_id: int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'UPDATE certificates SET person_id = %s WHERE id=2;'
        data = person_id
        cursor.execute(SQL, (data,)) # Pass argument to executor
        con.commit() # Save the changes to database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()    

# Remove an existing row from the person table. The id of the row to be deleted is taken as a function parameter
def rmv_person(id: int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'DELETE FROM person WHERE id=%s'
        data = id
        cursor.execute(SQL, (data,)) # Pass argument to executor
        con.commit() # Save the changes to database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()    

# Remove an existing row from the certificate table. The id of the row to be deleted is taken as a function parameter
def rmv_cert(id: int):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'DELETE FROM certificates WHERE id=%s'
        data = id
        cursor.execute(SQL, (data,)) # Pass argument to executor
        con.commit() # Save the changes to database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close() 

# Testing transactions
def transactions():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM accounts;'
        cursor.execute(SQL)
        row = cursor.fetchall()
        print("Testing connection to transactions:")
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()    

if __name__ == '__main__':
    connect()
    all_rows()
    column_names()
    cert_names_rows()
    avg_age()
    person_scrum()
    #db_create_person('Jenna', 26, False)
    #db_create_cert('Azure', 9)
    #test_person_function()
    #test_cert_function()
    #upd_person('Anna')
    #test_person_function()
    #upd_cert(10)
    #test_cert_function()
    #rmv_person(6)
    #test_person_function()
    #rmv_cert(4)
    #test_cert_function()
    transactions()