from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT students.name, ROUND(AVG(grades.grade), 2) AS avg_grade
    FROM students
    JOIN grades ON students.id = grades.student_id
    GROUP BY students.id
    ORDER BY avg_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.name, func.round(func.avg(Grade.grade), 2).label('avr_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('avr_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT students.name, ROUND(AVG(grades.grade), 2) AS avg_grade
    FROM students
    JOIN grades ON students.id = grades.student_id
    WHERE grades.subject_id = 1
    GROUP BY students.id
    ORDER BY avg_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.name, func.round(func.avg(Grade.grade), 2).label('avr_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subject_id == 1).group_by(Student.id).order_by(
        desc('avr_grade')).limit(1).all()
    return result

def select_03():
    """
    SELECT groups.name AS group_name, ROUND(AVG(grade), 2) AS avg_grade
    FROM groups
    JOIN students ON groups.id = students.group_id
    JOIN grades ON students.id = grades.student_id
    JOIN subjects ON grades.subject_id = subjects.id
    WHERE subjects.id = '2'
    GROUP BY groups.name;
    """
    result = session.query(Group.name.label('group_name'), func.round(func.avg(Grade.grade), 2).label('avr_grade')) \
        .select_from(Grade).join(Student).join(Group).join(Subject).filter(Subject.id ==        2).group_by(Group.name).order_by(
    desc('avr_grade')).all()

    return result
    

def select_04():
    """
    SELECT ROUND(AVG(grade), 2) AS avg_grade
    FROM grades;
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avr_grade')) \
        .select_from(Grade).all()
    return result


def select_05():
    """
    SELECT subjects.name AS course_name, teachers.name AS teacher_name
    FROM subjects
    JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE teachers.id = '2';
    """
    result = session.query(Subject.name.label('course_name'), Teacher.name.label('teacher_name')) \
        .select_from(Subject).join(Teacher).filter(Teacher.id == 2).all()
    return result

def select_06():
    """
    SELECT groups.name AS group_name, students.name AS student_name
    FROM students
    JOIN groups ON students.group_id = groups.id
    WHERE groups.id = '2';
    """
    result = session.query(Group.name.label('group_name'), Student.name.label('student_name'))\
        .select_from(Student).join(Group).filter(Group.id == 2).all()
    return result

def select_07():
    """
    SELECT groups.name AS group_, subjects.name AS subject,students.name AS student_name, grades.grade
    FROM students
    JOIN grades ON students.id = grades.student_id
    JOIN subjects ON grades.subject_id = subjects.id
    JOIN groups ON students.group_id = groups.id
    WHERE groups.id = '1' AND subjects.id = '3';
    """
    result = session.query(Group.name.label('group_name'), Subject.name.label('subject'), Student.name.label('student_name'), Grade.grade) \
        .select_from(Student).join(Grade).join(Subject).join(Group).filter(Group.id == 1, Grade.subject_id == 3).all()
    return result

def select_08():
    """
    SELECT teachers.name AS teacher_name,
       subjects.name AS subject,
       ROUND(AVG(grade), 2) AS avg_grade
    FROM grades
    JOIN subjects ON grades.subject_id = subjects.id
    JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE teachers.id = 1
    GROUP BY teachers.name, subjects.name;
    """
    result = session.query(Teacher.name.label('teacher_name'), Subject.name.label('subject'), func.round(func.avg(Grade.grade), 2).label('avr_grade')) \
        .select_from(Grade).join(Subject).join(Teacher).filter(Teacher.id == 1).group_by(          
            Teacher.name,
            Subject.name
        ).all()
    return result

def select_09():
    """
    SELECT DISTINCT subjects.name AS course_name, students.name AS student_name 
    FROM subjects
    JOIN grades ON subjects.id = grades.subject_id
    JOIN students ON grades.student_id = students.id
    WHERE students.id = '2';
    """
    result = session.query(Subject.name.label('course_name'), Student.name.label('student_name')) \
        .select_from(Subject).join(Grade).join(Student).filter(Student.id == 2).all()
    return result

def select_10():
    """
    SELECT DISTINCT subjects.name AS course_name, teachers.name AS teacher_name, students.name AS student_name 
    FROM subjects
    JOIN grades ON subjects.id = grades.subject_id
    JOIN students ON grades.student_id = students.id
    JOIN teachers ON subjects.teacher_id = teachers.id
    WHERE students.id = '3' AND teachers.id = '2';
    """
    result = session.query(Subject.name.label('subject'), Teacher.name.label('teacher_name'), Student.name.label('student_name')) \
        .select_from(Subject).join(Grade).join(Student).join(Teacher).filter(Student.id == 3, Teacher.id == 2).all()
    return result

def select_11():
    """
    SELECT AVG(grades.grade) AS average_grade
    FROM grades
    JOIN subjects ON grades.subject_id = subjects.id
    JOIN teachers ON subjects.teacher_id = teachers.id
    JOIN students ON grades.student_id = students.id
    WHERE teachers.id = '2' AND students.id = '2';
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avr_grade')) \
        .select_from(Grade).join(Subject).join(Teacher).join(Student).filter(Student.id == 2, Teacher.id == 2).all()
    return result


def select_12():
    """
    SELECT g.name AS group_,
       gr.grade_date AS _date,
       s.name AS course_name,
       st.name AS student_name,
       gr.grade
    FROM students st
    JOIN grades gr ON st.id = gr.student_id
    JOIN subjects s ON gr.subject_id = s.id
    JOIN groups g ON st.group_id = g.id
    WHERE g.id = 2 
    AND s.id = 2
    AND gr.grade_date = (
        SELECT MAX(grade_date)
        FROM grades g
        JOIN students st ON g.student_id = st.id
        JOIN groups gr ON st.group_id = gr.id
        WHERE gr.id = 2 
            AND g.subject_id = 2
  );
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subject_id == 2, Student.group_id == 2
    ))).scalar_subquery()

    result = session.query(Student.id, Student.name, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subject_id == 2, Student.group_id == 2, Grade.grade_date == subquery)).all()

    return result


if __name__ == '__main__':
    print(select_03())
    