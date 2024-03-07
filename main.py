import argparse
from datetime import datetime
import random

from conf.db import session
from conf.models import Grade, Student, Group, Subject, Teacher

import logging

from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)


def list(model):
    if model == "Grade":
        grades = session.query(Grade).all()
        res = ''
        for grade in grades:
            subject = session.query(Subject).filter_by(id=grade.subject_id).first()
            student = session.query(Student).filter_by(id=grade.student_id).first()
            res += f"ID: {grade.id} Grade: {grade.grade} Date: {grade.grade_date} Subject: {subject.name}, Student: {student.name}\n" 
        print(res)
    elif model in ["Student", "Teacher", "Group", "Subject"]:
        result = session.query(eval(model)).all()
        for res in result:
            print(f"ID: {res.id} {model}_name: {res.name}")


def create(model, name):
    if model == "Grade":
        student_ids = session.query(Student.id).all()
        subject_ids = session.query(Subject.id).all()
        res = eval(model)(grade=name, student_id=random.choice(student_ids)[0],
            subject_id=random.choice(subject_ids)[0], grade_date=datetime.today())
    elif model == "Teacher":
        res = eval(model)(name=name)
    elif model == "Student":
        _id = session.query(Group.id).all()
        res = eval(model)(name=name, group_id=random.choice(_id)[0])
    elif model == "Group":
        res = eval(model)(name=name)
    elif model == "Subject":
        _id = session.query(Teacher.id).all()
        res = eval(model)(name=name, teacher_id=random.choice(_id)[0])
        
    try:
        session.add(res)
        session.commit()
        logging.info(f'{model}: "{name}" successfully added.')
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
    finally:
        session.close()


def update(model, id_, name):
    res = session.query(eval(model)).get(id_)
    if res:
        if model in ["Teacher", "Group", "Student", "Subject"]:
            res.name = name
        elif model == "Grade":
            res.grade = name

        try:
            session.add(res)
            logging.info(
                f"Table: {model} ID: {id_} was successfully updated."
            )
            session.commit()
        except SQLAlchemyError as e:
            print(e)
            session.rollback()
        finally:
            session.close()


def remove(model, id_):
    res = session.query(eval(model)).get(id_)
    try:
        session.delete(res)
        logging.info(f"ID: {id_} was successfully deleted from {model}")
        session.commit()
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="APP")
    parser.add_argument("-a", "--action", choices=["create", "list", "update", "remove"],
        help="Command: create, list, update, remove", required=True)
    parser.add_argument("-m", "--model", choices=["Teacher", "Group", "Grade", "Student", "Subject"],
        help="Model: Teacher, Group, Grade, Student, Subject",required=True)
    parser.add_argument("-id", "--id", type=int, help="ID")
    parser.add_argument("-n", "--name", help="Name")

    args = parser.parse_args()

    if args.action == "create":
        if args.name:
            create(args.model, name=args.name)
    elif args.action == "list":
        list(args.model)
    elif args.action == "update":
        if args.id and args.name:
            update(args.model, id_=args.id, name=args.name)
    elif args.action == "remove":
        if args.id:
            remove(args.model, args.id)


if __name__ == "__main__":
    main()
