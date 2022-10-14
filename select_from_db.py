from create_db import *
from fill_db import *

if __name__ == '__main__':

    sql_select_five_highest = " SELECT round(AVG(m.mark)), s.first_name, s.last_name " \
                              "FROM marks m " \
                              "LEFT JOIN students s " \
                              "ON m.student_id = s.id " \
                              "GROUP BY s.first_name, s.last_name " \
                              "ORDER BY AVG(m.mark)" \
                              "DESC LIMIT 5;"

    sql_select_one_highest = "SELECT dis.discipline, round(avg(m.mark)), s.first_name, s.last_name " \
                             "FROM marks m " \
                             "JOIN students s ON m.student_id = s.id " \
                             "JOIN disciplines dis ON m.discipline_id = dis.id  " \
                             "GROUP BY s.first_name, s.last_name, dis.id, dis.discipline  " \
                             "ORDER BY dis.id " \
                             "DESC LIMIT 1;"

    sql_select_group_average = "SELECT avg(m.mark) " \
                               "FROM marks m " \
                               "JOIN students as s ON m.student_id = s.id " \
                               "GROUP BY s.group_id; "

    sql_select_average_mark = "SELECT avg(m.mark) " \
                              "FROM marks m " \
                              "JOIN students as s on m.student_id = s.id " \
                              "GROUP BY s.group_id;"

    sql_select_teacher_courses = "SELECT s.discipline, t.last_name " \
                                 "FROM disciplines s " \
                                 "JOIN teachers as t ON s.teacher_id = t.id;"

    sql_select_group_students = "SELECT s.first_name, s.last_name " \
                                "FROM students s " \
                                "WHERE s.group_id = 1;"

    sql_select_discipline_marks = "SELECT s.first_name, s.last_name, m.mark, g.title, dis.discipline " \
                                  "FROM students s " \
                                  "JOIN marks m ON m.student_id = s.id  " \
                                  "JOIN groups g ON s.group_id = g.id  " \
                                  "JOIN disciplines dis ON m.discipline_id  = dis.id  " \
                                  "WHERE s.group_id = 2 and m.discipline_id = 1 " \
                                  "ORDER BY m.mark DESC;"

    sql_select_last_marks = "SELECT max(m.lesson_date), s.first_name, s.last_name, g.title, dis.discipline " \
                            "FROM marks m " \
                            "LEFT JOIN students as s ON s.id = m.student_id " \
                            "LEFT JOIN groups g ON g.id = s.group_id  " \
                            "LEFT JOIN disciplines dis ON dis.id = m.discipline_id  " \
                            "WHERE m.discipline_id = 3 and g.id = 2" \
                            "GROUP BY s.first_name, s.last_name, g.title, dis.discipline;"

    sql_select_student_disciplines = "SELECT DISTINCT s.id, s.first_name, s.last_name, dis.discipline " \
                                     "FROM students s  " \
                                     "JOIN marks m ON m.student_id  = s.id  " \
                                     "JOIN disciplines dis ON dis.id = m.discipline_id  " \
                                     "WHERE s.id = 3"

    sql_select_student_teacher = "SELECT DISTINCT s.id, s.first_name, s.last_name, dis.discipline, t.last_name " \
                                 "FROM students s " \
                                 "JOIN marks m ON m.student_id  = s.id " \
                                 "JOIN disciplines dis ON dis.id = m.discipline_id " \
                                 "JOIN teachers t ON m.teacher_id = t.id " \
                                 "WHERE s.id = 1 and t.id = 2"

    sql_select_student_mark = "SELECT round(avg(m.mark)),s.first_name, s.last_name " \
                              "FROM students s " \
                              "JOIN marks m ON m.student_id = s.id " \
                              "JOIN teachers t ON t.id = m.teacher_id " \
                              "WHERE s.id = 1" \
                              "GROUP BY s.first_name, s.last_name;"

    sql_select_teacher_mark = "SELECT ROUND(AVG(m.mark)), t.first_name, t.last_name " \
                              "FROM teachers t " \
                              "JOIN marks m  ON teacher_id = m.teacher_id  " \
                              "WHERE  t.id = 3" \
                              "GROUP BY t.first_name, t.last_name;"
    tasks = [
        sql_select_five_highest, sql_select_one_highest, sql_select_group_average, sql_select_average_mark,
        sql_select_teacher_courses, sql_select_group_students, sql_select_discipline_marks, sql_select_last_marks,
        sql_select_student_disciplines, sql_select_student_teacher, sql_select_student_mark, sql_select_teacher_mark
    ]

    with create_connection() as conn:
        if conn is not None:
            for task in tasks:
                cur = conn.cursor()
                cur.execute(task)
                print(cur.fetchall())
                cur.close()
        else:
            print('Error: can\'t create the database connection')