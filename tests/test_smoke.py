import inspect

from flows.term.term_flow import term_raw_flow
from flows.course.course_flow import course_raw_flow
from flows.person.person_flow import person_raw_flow


def test_term_flow_is_callable():
    assert callable(term_raw_flow)


def test_term_flow_has_test_run():
    sig = inspect.signature(term_raw_flow.fn)
    assert "test_run" in sig.parameters
    assert sig.parameters["test_run"].default is False


def test_course_flow_is_callable():
    assert callable(course_raw_flow)


def test_course_flow_has_test_run():
    sig = inspect.signature(course_raw_flow.fn)
    assert "test_run" in sig.parameters
    assert sig.parameters["test_run"].default is False


def test_person_flow_is_callable():
    assert callable(person_raw_flow)


def test_person_flow_has_test_run():
    sig = inspect.signature(person_raw_flow.fn)
    assert "test_run" in sig.parameters
    assert sig.parameters["test_run"].default is False
