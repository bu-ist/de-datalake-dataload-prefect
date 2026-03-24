"""
Prefect flows for data lake loading.
"""
from flows.term.term_flow import term_raw_flow
from flows.course.course_flow import course_raw_flow
from flows.person.person_flow import person_raw_flow

__all__ = [
    "term_raw_flow",
    "course_raw_flow",
    "person_raw_flow",
]

