import unittest
from src.raft_node import RaftNode, State, LogEntry
from src.rpc_provider import RPCClient

class MockRPC(RPCClient):
    def send_request_vote(self, port, args):
        return {'term': 1, 'voteGranted': True}
    def send_append_entries(self, port, args):
        return {'term': 1, 'success': True}

class TestRaftRules(unittest.TestCase):
    def setUp(self):
        # 0 peers
        self.node = RaftNode(0, 0, {})
        self.node.rpc_client = MockRPC()
        # 清理持久化
        self.node.persister.save(0, None, [])

    def test_vote_rejection_low_term(self):
        """Rule: Reply false if term < currentTerm [cite: 215]"""
        self.node.current_term = 2
        reply = self.node.handle_request_vote({
            'term': 1, 'candidateId': 1, 'lastLogIndex': 0, 'lastLogTerm': 0
        })
        self.assertFalse(reply['voteGranted'])

    def test_vote_rejection_log_not_uptodate(self):
        """Rule: Candidate's log must be at least as up-to-date [cite: 216]"""
        # Node has term 2 log
        self.node.current_term = 2
        self.node.log = [LogEntry(1, 0, "c"), LogEntry(2, 1, "c")]
        
        # Candidate has term 1 log (older)
        reply = self.node.handle_request_vote({
            'term': 3, 'candidateId': 1, 'lastLogIndex': 1, 'lastLogTerm': 1
        })
        self.assertFalse(reply['voteGranted'])

    def test_log_matching_rejection(self):
        """Rule: Reply false if log doesn't contain entry at prevLogIndex [cite: 196]"""
        self.node.log = [LogEntry(1, 0, "cmd1")]
        # Leader claims prevLogIndex is 1, but we only have 0
        reply = self.node.handle_append_entries({
            'term': 1, 'leaderId': 1, 'prevLogIndex': 1, 'prevLogTerm': 1,
            'entries': [], 'leaderCommit': 0
        })
        self.assertFalse(reply['success'])

if __name__ == '__main__':
    unittest.main()