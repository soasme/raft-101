import unittest
from unittest.mock import patch, MagicMock

from raft import *

def mock_keep_receiving(values):
    values = iter(values)
    def keep_receiving(server, second):
        try:
            value = next(values)
        except StopIteration:
            value = None
        return Stream(value, lambda: keep_receiving(server, second), exit=value is None)
    return keep_receiving

class TestRaft(unittest.TestCase):

    @property
    def init_state(self):
        return init_raft_state({
            'bind': '0.0.0.0:8000',
            'peers': ['0.0.0.0:8001', '0.0.0.0:8002'],
            'udp_server': MagicMock(),
        })

    def test_follower_convert_to_candidate(self):
        # receive nothing to simulate timeout
        with patch('raft.keep_receiving', side_effect=mock_keep_receiving([])):
            stream = make_raft_server(self.init_state)
            assert stream.head.ctx['server_state'] == 'follower' # be a follower first.
            assert stream.rest.head.ctx['server_state'] == 'candidate' # become a candidate then.

    def test_follower_keep_server_state_after_receiving_append_entries(self):
        with patch('raft.keep_receiving', side_effect=mock_keep_receiving([
            Datagram('0.0.0.0:8001', {
                'type': 'append_entries',
                'term': 1,
                'leader_id': '0.0.0.0:8001',
                'prev_log_index': 1,
                'prev_log_term': 1,
                'leader_commit': 1,
                'entries': [],
            })
        ])):
            stream = make_raft_server(self.init_state)
            assert stream.head.ctx['server_state'] == 'follower' # be a follower first.
            assert stream.rest.head.ctx['server_state'] == 'follower' # ***keep a follower then***
            assert stream.rest.rest.head.ctx['server_state'] == 'candidate' # become a candidate then.

    def test_follower_keep_server_state_after_receiving_request_vote(self):
        with patch('raft.keep_receiving', side_effect=mock_keep_receiving([
            Datagram('0.0.0.0:8001', {
                'type': 'request_vote',
                'term': 1,
                'candidate_id': '0.0.0.0:8001',
                'last_log_index': 1,
                'last_log_term': 0,
            })
        ])):
            stream = make_raft_server(self.init_state)
            assert stream.head.ctx['server_state'] == 'follower'
            assert stream.rest.head.ctx['server_state'] == 'follower' # *** keep follower state ***
            assert stream.rest.head.voted_for == '0.0.0.0:8001' # *** vote for candidate ***
            assert stream.rest.rest.head.ctx['server_state'] == 'candidate'

    def test_candidate_restart_election(self):
        with patch('raft.keep_receiving', side_effect=mock_keep_receiving([])):
            state = self.init_state.to(
                current_term=1,
                ctx=dict(self.init_state.ctx, server_state='candidate'),
            )
            stream = make_raft_server(state)
            assert stream.head.ctx['server_state'] == 'candidate'
            assert stream.rest.head.current_term == 2
            assert stream.rest.head.ctx['server_state'] == 'candidate'

    def test_candidate_receive_majority_votes(self):
        with patch('raft.keep_receiving', side_effect=mock_keep_receiving([
            None,
            Datagram('0.0.0.0:8001', {
                'type': 'request_vote_response',
                'term': 1,
                'vote_granted': True,
            })
        ])):
            stream = make_raft_server(self.init_state)
            assert stream.head.ctx['server_state'] == 'follower'
            assert stream.rest.head.ctx['server_state'] == 'leader'
            assert stream.rest.rest.head.ctx['server_state'] == 'leader'

    def test_candidate_does_not_receive_majority_votes(self):
        with patch('raft.keep_receiving', side_effect=mock_keep_receiving([
            None,
            Datagram('0.0.0.0:8001', {
                'type': 'request_vote_response',
                'term': 1,
                'vote_granted': False,
            })
        ])):
            stream = make_raft_server(self.init_state)
            assert stream.head.ctx['server_state'] == 'follower'
            assert stream.rest.head.ctx['server_state'] == 'candidate'
            assert stream.rest.rest.head.ctx['server_state'] == 'follower'

    def test_on_next_callback(self):
        with patch('raft.keep_receiving', side_effect=mock_keep_receiving([])):
            n = 1
            def on_next(state):
                nonlocal n
                n += 1
                return state
            state = self.init_state.to(ctx=dict(self.init_state.ctx, on_next=on_next))
            stream = make_raft_server(state)
            assert stream.head.ctx['server_state'] == 'follower'
            assert n == 2
            assert stream.rest.head.ctx['server_state'] == 'candidate'
            assert n == 3


if __name__ == '__main__':
    unittest.main()
