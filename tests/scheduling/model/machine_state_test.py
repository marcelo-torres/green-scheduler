import unittest

from src.scheduling.model.machine import Machine
from src.scheduling.model.machine_state import MachineState


def create_state(cores):
    machine = Machine('id', cores=cores)
    return MachineState(machine)


def _get_state_1():
    '''
        |   time      |  cpus used  | cpus free |
        | [  0s,  5s]  |       5     |    15     |
        | [  5s,  9s]  |       4     |    13     |
        | [  9s, 14s]  |       2     |    18     |
        | [ 14s, 16s]  |       0     |    20     |
        | [ 16s, 20s]  |      19     |    01     |
    :return:
    '''
    state = create_state(20)
    state.use_cores(5, 4, 7)
    state.use_cores(16, 4, 19)
    state.use_cores(0, 5, 5)
    state.use_cores(9, 5, 2)

    return state


class MachineStateTest(unittest.TestCase):

    def test_min_free_cores_no_usage(self):
        state = create_state(10)
        self.assertEqual(10, state.min_free_cores_in(0, 20))

    def test_core_usages_at_same_time(self):
        state = create_state(10)
        state.use_cores(2, 3, 6)
        state.use_cores(2, 3, 1)
        self.assert_min_cores_in(state, 10, 0, 2)
        self.assert_min_cores_in(state, 3, 2, 5)
        self.assert_min_cores_in(state, 10, 5, 20)

    def test_if_no_cores_available_throw_exception(self):
        state = create_state(10)
        with self.assertRaises(Exception):
            state.use_cores(1, 2, 11)

    def test_min_free_cores_single_usage(self):
        state = create_state(10)
        state.use_cores(5, 10, 6)
        self.assert_min_cores_in(state, 4, 0, 20)
        self.assert_min_cores_in(state, 10, 0, 5)
        self.assert_min_cores_in(state, 4, 5, 15)
        self.assert_min_cores_in(state, 10, 15, 20)

    def test_min_free_cores_usage_all_interval_not_overlapping(self):
        state = _get_state_1()

        self.assert_min_cores_in(state, 15, 0, 5)
        self.assert_min_cores_in(state, 13, 5, 9)
        self.assert_min_cores_in(state, 18, 9, 14)
        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 20)

    def test_min_free_cores_overlap_first_interval(self):
        state = _get_state_1()

        state.use_cores(1, 2, 15)

        self.assert_min_cores_in(state, 15, 0, 1)
        self.assert_min_cores_in(state, 0, 1, 3)
        self.assert_min_cores_in(state, 15, 3, 5)

        self.assert_min_cores_in(state, 13, 5, 9)
        self.assert_min_cores_in(state, 18, 9, 14)
        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 20)

    def test_min_free_cores_overlap_first_start(self):
        state = _get_state_1()

        state.use_cores(0, 2, 14)

        self.assert_min_cores_in(state, 1, 0, 2)
        self.assert_min_cores_in(state, 15, 2, 5)

        self.assert_min_cores_in(state, 13, 5, 9)
        self.assert_min_cores_in(state, 18, 9, 14)
        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 20)

    def test_min_free_cores_overlap_last_interval(self):
        state = _get_state_1()

        state.use_cores(17, 2, 1)

        self.assert_min_cores_in(state, 15, 0, 5)
        self.assert_min_cores_in(state, 13, 5, 9)
        self.assert_min_cores_in(state, 18, 9, 14)
        self.assert_min_cores_in(state, 20, 14, 16)

        self.assert_min_cores_in(state, 1, 16, 17)
        self.assert_min_cores_in(state, 0, 17, 19)
        self.assert_min_cores_in(state, 1, 19, 20)

    def test_min_free_cores_overlap_end(self):
        state = _get_state_1()

        state.use_cores(19, 1, 1)

        self.assert_min_cores_in(state, 15, 0, 5)
        self.assert_min_cores_in(state, 13, 5, 9)
        self.assert_min_cores_in(state, 18, 9, 14)
        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 19)
        self.assert_min_cores_in(state, 0, 19, 20)

    def test_free_cores_more_than_available(self):
        state = create_state(10)
        with self.assertRaises(Exception):
            state.free_cores(0, 1, 11)

    def test_min_cores_of_single_time_unit(self):
        state = create_state(10)
        self.assert_min_cores_in(state, 10, 0, 0)
        self.assert_min_cores_in(state, 10, 1, 1)
        self.assert_min_cores_in(state, 10, 20, 20)

    def test_free_all_cores(self):
        state = create_state(10)
        state.use_cores(0, 20, 10)
        state.free_cores(0, 20, 10)
        self.assert_min_cores_in(state, 10, 0, 20)

    def test_free_cores_in_middle_of_interval(self):
        state = _get_state_1()
        state.free_cores(11, 2, 1)

        self.assert_min_cores_in(state, 15, 0, 5)
        self.assert_min_cores_in(state, 13, 5, 9)

        self.assert_min_cores_in(state, 18, 9, 11)
        self.assert_min_cores_in(state, 19, 11, 13)
        self.assert_min_cores_in(state, 18, 13, 14)

        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 20)

    def test_free_cores_splitting_an_interval(self):
        state = _get_state_1()
        state.free_cores(4, 3, 4)

        self.assert_min_cores_in(state, 15, 0, 4)
        self.assert_min_cores_in(state, 19, 4, 5)
        self.assert_min_cores_in(state, 17, 5, 7)
        self.assert_min_cores_in(state, 13, 7, 9)

        self.assert_min_cores_in(state, 18, 9, 14)
        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 20)

    def test_free_cores_n_intervals(self):
        state = _get_state_1()
        state.free_cores(0, 14, 1)

        self.assert_min_cores_in(state, 16, 0, 5)
        self.assert_min_cores_in(state, 14, 5, 9)
        self.assert_min_cores_in(state, 19, 9, 14)
        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 20)

    def test_free_cores_usage_start(self):
        state = _get_state_1()
        state.free_cores(0, 7, 2)

        self.assert_min_cores_in(state, 17, 0, 5)
        self.assert_min_cores_in(state, 15, 5, 7)
        self.assert_min_cores_in(state, 13, 7, 9)

        self.assert_min_cores_in(state, 18, 9, 14)
        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 20)

    def test_free_cores_usage_end(self):
        state = _get_state_1()
        state.free_cores(7, 7, 1)

        self.assert_min_cores_in(state, 15, 0, 5)

        self.assert_min_cores_in(state, 13, 5, 7)
        self.assert_min_cores_in(state, 14, 7, 9)
        self.assert_min_cores_in(state, 19, 9, 14)

        self.assert_min_cores_in(state, 20, 14, 16)
        self.assert_min_cores_in(state, 1, 16, 20)

    def test_next_key(self):
        state = _get_state_1()

        self.assertEqual(5, state.next_start(0))
        self.assertEqual(5, state.next_start(1))
        self.assertEqual(5, state.next_start(4))

        self.assertEqual(9, state.next_start(5))

    def test_previous_key(self):
        state = _get_state_1()

        self.assertEqual(0, state.previous_start(1))
        self.assertEqual(0, state.previous_start(5))

        self.assertEqual(5, state.previous_start(6))
        self.assertEqual(5, state.previous_start(9))

        self.assertEqual(16, state.previous_start(20))

    def test_bug_start_not_added(self):
        state = create_state(3)

        state.use_cores(173, 13, 1)
        state.use_cores(171, 2, 1)
        state.use_cores(170, 3, 1)
        state.use_cores(166, 7, 1)

        state.free_cores(173, 13, 1)
        state.free_cores(171, 2, 1)
        state.free_cores(170, 3, 1)
        state.free_cores(166, 7, 1)

        self.assert_min_cores_in(state, 3, 0, 186)

    def test_search_full_interval_available(self):
        state = create_state(3)

        intervals = list(
            state.search_intervals_with_free_cores(0, 20, 10, 1)
        )

        self.assertEqual(1, len(intervals))

        self.assert_interval(intervals[0], 0, 20)

    def test_search_full_interval_available_cores_exactly(self):
        state = create_state(3)

        intervals = list(
            state.search_intervals_with_free_cores(0, 20, 10, 3)
        )

        self.assertEqual(1, len(intervals))

        self.assert_interval(intervals[0], 0, 20)

    def test_search_full_interval_not_available(self):
        state = create_state(3)

        intervals = list(
            state.search_intervals_with_free_cores(0, 20, 10, 4)
        )

        self.assertEqual(0, len(intervals))

    def test_search_no_interval_available(self):
        state = _get_state_1()

        intervals = list(
            state.search_intervals_with_free_cores(0, 20, 1, 21)
        )

        self.assertEqual(0, len(intervals))

    def test_search_interval_by_end_of_other(self):
        state = create_state(10)

        state.use_cores(0, 5, 10)
        state.use_cores(10, 5, 10)
        state.use_cores(20, 5, 10)

        intervals = list(
            state.search_intervals_with_free_cores(0, 30, 1, 9)
        )

        self.assertEqual(3, len(intervals))

        self.assert_interval(intervals[0], 5, 10)
        self.assert_interval(intervals[1], 15, 20)
        self.assert_interval(intervals[2], 25, 30)

    def test_search_interval_that_spans_over_events(self):
        state = _get_state_1()

        intervals = list(
            state.search_intervals_with_free_cores(0, 20, 1, 14)
        )

        self.assertEqual(2, len(intervals))

        self.assert_interval(intervals[0], 0, 5)
        self.assert_interval(intervals[1], 9, 16)

    def test_search_interval_with_last_interval_with_not_enough_cores(self):
        state = create_state(10)

        state.use_cores(0, 5, 9)
        state.use_cores(5, 5, 8)
        state.use_cores(10, 5, 7)

        state.use_cores(15, 5, 10)

        state.use_cores(20, 2, 9)
        state.use_cores(22, 2, 8)

        intervals = list(
            state.search_intervals_with_free_cores(0, 24, 5, 2)
        )

        self.assertEqual(1, len(intervals))

        self.assert_interval(intervals[0], 5, 15)

    def assert_min_cores_in(self, state, min_cores, start, end):
        self.assertEqual(min_cores, state.min_free_cores_in(start, end))

    def assert_interval(self, interval, expected_start, expected_end):
        self.assertEqual(interval[0], expected_start)
        self.assertEqual(interval[1], expected_end)
