import random
from random import randint
import psycopg2
from contextlib import contextmanager
from sqlite3 import Error
from faker import Faker
fake = Faker()

@contextmanager
def create_connection():

    conn = None
    try:
        conn = psycopg2.connect(host='localhost', database='hw', user='postgres', password='mysecretpassword')
        yield conn
        conn.commit()
    except Error as e:
        print(e)
        conn.rollback()
    finally:
        conn.close()


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

if __name__ == '__main__':
    sql_create_groups_table = """CREATE TABLE IF NOT EXISTS groups (
    id SERIAL PRIMARY KEY,
    title VARCHAR(30)
    );"""

    sql_create_students_table = """CREATE TABLE IF NOT EXISTS students (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    group_id INTEGER,
    FOREIGN KEY (group_id) REFERENCES groups (id)
          ON DELETE CASCADE
          ON UPDATE CASCADE
    );"""

    sql_create_teacher_table = """CREATE TABLE IF NOT EXISTS teachers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(30),
    last_name VARCHAR(30)
    );"""

    sql_create_disciplines_table = """CREATE TABLE IF NOT EXISTS disciplines (
    id SERIAL PRIMARY KEY,
    discipline VARCHAR(30),
    teacher_id INTEGER,
    group_id INTEGER,
    FOREIGN KEY (group_id) REFERENCES groups (id)
          ON DELETE CASCADE
          ON UPDATE CASCADE,
     FOREIGN KEY (teacher_id) REFERENCES teachers (id)
          ON DELETE CASCADE
          ON UPDATE CASCADE
    );"""

    sql_create_marks_table = """CREATE TABLE IF NOT EXISTS marks (
    id SERIAL PRIMARY KEY,
    mark INTEGER,
    lesson_date DATE,
    teacher_id INTEGER,
    student_id INTEGER,
    discipline_id INTEGER,
    FOREIGN KEY (discipline_id) REFERENCES disciplines (id)
          ON DELETE CASCADE
          ON UPDATE CASCADE,
    FOREIGN KEY (teacher_id) REFERENCES teachers (id)
          ON DELETE CASCADE
          ON UPDATE CASCADE,
    FOREIGN KEY (student_id) REFERENCES students (id)
          ON DELETE CASCADE
          ON UPDATE CASCADE
    );"""

    Tables = [sql_create_groups_table, sql_create_students_table, sql_create_teacher_table, sql_create_disciplines_table, sql_create_marks_table]

    with create_connection() as conn:
        if conn is not None:
            for table in Tables:
                create_table(conn, table)
        else:
            print('Error: can\'t create the database connection')
