"""
Prefect flows for data lake loading.
"""
from flows.term_raw_flow import term_raw_flow
from flows.course_raw_flow import course_raw_flow
from flows.person_raw_flow import person_raw_flow

__all__ = [
    "term_raw_flow",
    "course_raw_flow",
    "person_raw_flow",
]

