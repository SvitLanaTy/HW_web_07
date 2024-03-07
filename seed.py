from random import randint, choice

from faker import Faker
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from conf.models import Teacher, Group, Student, Subject, Grade
from conf.db import session

fake = Faker()

subjects = [
    "Mathematics",
    "Physics",
    "Chemistry",
    "Biology",
    "History",
    "Literature",
    "Computer Science"
]

groups = ["Group A", "Group B", "Group C"]

def create_teachers():
    for _ in range(3):
        teacher = Teacher(name=fake.name())
        session.add(teacher)
   
   
def create_groups():
    
    for group in groups:
        session.add(Group(name=group))

def create_subjects():
    teacher_ids = session.scalars(select(Teacher.id)).all()
    for subject_name in subjects:
        subject = Subject(name=subject_name, teacher_id=choice(teacher_ids))
        session.add(subject)


def create_students():
     group_ids = session.scalars(select(Group.id)).all()
     for _ in range(10):
        student = Student(name=fake.name(), group_id=choice(group_ids))
        session.add(student)



def create_grades():
    students = session.query(Student).all()
    subjects = session.query(Subject).all()

    for student in students:
        for subject in subjects:
            grade = Grade(
                grade=randint(0, 100),
                grade_date=fake.date_this_decade(),
                student_id=student.id,
                subject_id=subject.id                
            )
            session.add(grade)            
    

if __name__ == '__main__':
    try:
        create_teachers()        
        create_groups()
        create_subjects()
        create_students()
        create_grades()
        session.commit()
    except SQLAlchemyError as e:        
        print(e)
        session.rollback()
    finally:
        session.close()
