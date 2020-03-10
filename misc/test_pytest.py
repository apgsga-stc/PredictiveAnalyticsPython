from pytest import approx


def test_approx():
    assert (0.1 + 0.2, 0.2 + 0.4) == approx((0.3, 0.6))
