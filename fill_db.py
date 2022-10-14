from create_db import *

if __name__ == '__main__':
    sql_insert_groups_table = "INSERT INTO groups(title) VALUES(%s)"
    sql_insert_students_table = "INSERT INTO students(first_name, last_name, group_id) VALUES(%s, %s, %s)"
    sql_insert_teachers_table = "INSERT INTO teachers(first_name, last_name) VALUES(%s, %s)"
    sql_insert_disciplines_table = "INSERT INTO disciplines(discipline, teacher_id, group_id) VALUES(%s, %s, %s)"
    sql_insert_marks_table = "INSERT INTO marks(mark, lesson_date, teacher_id, student_id, discipline_id) VALUES(%s, %s, %s, %s, %s)"


    with create_connection() as conn:
        if conn is not None:
            cur = conn.cursor()
            for _ in range(4):
                cur.execute(sql_insert_groups_table, (random.choice(['A', 'B', 'C', 'D'])))
        else:
            print('Error: can\'t create the database connection')

    with create_connection() as conn:
        if conn is not None:
            cur = conn.cursor()
            for _ in range(30):
                cur.execute(sql_insert_students_table, (fake.first_name(), fake.last_name(), random.randint(1, 4)))
        else:
            print('Error: can\'t create the database connection')

    with create_connection() as conn:
        if conn is not None:
            cur = conn.cursor()
            for _ in range(3):
                cur.execute(sql_insert_teachers_table, (fake.first_name(), fake.last_name()))
        else:
            print('Error: can\'t create the database connection')

    with create_connection() as conn:
        if conn is not None:
            cur = conn.cursor()
            for _ in range(5):
                cur.execute(sql_insert_disciplines_table, (random.choice(['Python', 'Java', 'QA', 'C++']), random.randint(1, 3), random.randint(1, 4)))
        else:
            print('Error: can\'t create the database connection')

    with create_connection() as conn:
        if conn is not None:
            cur = conn.cursor()
            for _ in range(600):
                cur.execute(sql_insert_marks_table, (random.randint(1,5), fake.date(), random.randint(1, 3), random.randint(1, 30), random.randint(1, 5)))
        else:
            print('Error: can\'t create the database connection')