import pytest
import socket

# Test "approximately equal" functionality
def test_approx():
    assert (0.1 + 0.2, 0.2 + 0.4) == pytest.approx((0.3, 0.6))


# This test will be skipped
@pytest.mark.skip(reason="Will fail")
def test_skipped():
    assert 1 == 2


# This test will run 3 times with different "number" parameters
@pytest.mark.parametrize("number", [1, 2, 3])
def test_positive(number: int):
    assert number > 0


# This test is expected to fail throwing a ZeroDivisionError exception
@pytest.mark.xfail(strict=True, reason="Division by zero", raises=ZeroDivisionError)
def test_fail():
    _ = 1 / 0


# This test has a custom mark, such that it can be run/skipped in some situations
# mark is defined in "pytest.ini" file, can be referenced with -m runtime flag:
# pytest -m 'not in_house'
@pytest.mark.in_house
def test_run_inhouse():
    domain = socket.getfqdn().split(".", maxsplit=1)[1]
    assert domain == "affichage-p.ch"
