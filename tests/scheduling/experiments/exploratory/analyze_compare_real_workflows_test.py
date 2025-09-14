import pytest

from src.experiments.exploratory.analyze_compare_real_workflows import _compute_workflows_average


def test_compute_workflows_average_single_result():
    """Testa se retorna os mesmos valores quando há apenas um resultado."""
    results_list = [
        {
            "lpt": {"brown_energy_used": 10, "makespan": 20, "start_time": 30},
            "bbs-shift-left": {"brown_energy_used": 5, "makespan": 15, "start_time": 25},
        }
    ]

    averages = _compute_workflows_average(results_list)

    assert set(averages.keys()) == {"lpt", "bbs-shift-left"}
    assert averages["lpt"]["brown_energy_used"] == 10
    assert averages["lpt"]["makespan"] == 20
    assert averages["lpt"]["start_time"] == 30
    assert averages["bbs-shift-left"]["brown_energy_used"] == 5


def test_compute_workflows_average_multiple_results():
    """Testa se calcula corretamente a média para múltiplos resultados."""
    results_list = [
        {"lpt": {"brown_energy_used": 10, "makespan": 20, "start_time": 30}},
        {"lpt": {"brown_energy_used": 20, "makespan": 40, "start_time": 50}},
    ]

    averages = _compute_workflows_average(results_list)

    assert "lpt" in averages
    assert averages["lpt"]["brown_energy_used"] == pytest.approx(15.0)
    assert averages["lpt"]["makespan"] == pytest.approx(30.0)
    assert averages["lpt"]["start_time"] == pytest.approx(40.0)


def test_compute_workflows_average_multiple_algorithms():
    """Testa se suporta múltiplos algoritmos e agrega corretamente."""
    results_list = [
        {
            "lpt": {"brown_energy_used": 10, "makespan": 20, "start_time": 30},
            "task_flow": {"brown_energy_used": 100, "makespan": 200, "start_time": 300},
        },
        {
            "lpt": {"brown_energy_used": 30, "makespan": 40, "start_time": 50},
            "task_flow": {"brown_energy_used": 300, "makespan": 400, "start_time": 500},
        },
    ]

    averages = _compute_workflows_average(results_list)

    assert set(averages.keys()) == {"lpt", "task_flow"}
    assert averages["lpt"]["brown_energy_used"] == pytest.approx((10 + 30) / 2)
    assert averages["task_flow"]["makespan"] == pytest.approx((200 + 400) / 2)


