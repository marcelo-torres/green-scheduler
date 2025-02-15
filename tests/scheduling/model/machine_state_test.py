import unittest

from src.scheduling.model.machine import Machine
from src.scheduling.model.machine_state import MachineState

def create_state(cores, deadline):
    machine = Machine('id', cores=cores)
    return MachineState(machine, deadline)

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
    state = create_state(20, 20)
    state.use_cores(5, 4, 7)
    state.use_cores(16, 4, 19)
    state.use_cores(0, 5, 5)
    state.use_cores(9, 5, 2)

    return state


class MachineStateTest(unittest.TestCase):

    def test_min_free_cores_no_usage(self):
        state = create_state(10, 20)
        self.assertEqual(10, state.min_free_cores_in(0, 20))

    def test_if_no_cores_available_throw_exception(self):
        state = create_state(10, 20)
        with self.assertRaises(Exception):
            state.use_cores(1, 2, 11)

    def test_min_free_cores_single_usage(self):
        state = create_state(10, 20)
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

    def assert_min_cores_in(self, state, min_cores, start, end):
        self.assertEqual(min_cores, state.min_free_cores_in(start, end))
