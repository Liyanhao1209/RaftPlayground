import unittest
import multiprocessing
import time
import os
import shutil
from src.raft_node import RaftNode
from src.config import BASE_PORT

class TestRaftIntegration(unittest.TestCase):
    def setUp(self):
        self.processes = []
        self.queues = [] # (status_queue, command_queue)
        self.n_nodes = 3
        
        # 清理数据
        if os.path.exists('data'):
            shutil.rmtree('data')

        peers = {i: BASE_PORT + i for i in range(self.n_nodes)}
        
        for i in range(self.n_nodes):
            sq = multiprocessing.Queue()
            cq = multiprocessing.Queue()
            self.queues.append((sq, cq))
            
            p = multiprocessing.Process(
                target=self.run_node, 
                args=(i, BASE_PORT + i, peers, sq, cq)
            )
            p.start()
            self.processes.append(p)

    def run_node(self, node_id, port, peers, sq, cq):
        # 剔除自己
        my_peers = {k:v for k,v in peers.items() if k != node_id}
        node = RaftNode(node_id, port, my_peers, sq, cq)
        node.start()
        while True: time.sleep(1)

    def tearDown(self):
        for p in self.processes:
            p.terminate()
            p.join()

    def test_leader_election(self):
        """测试集群能选出Leader"""
        print("\nWaiting for leader election...")
        start = time.time()
        leader_found = False
        while time.time() - start < 10:
            for i in range(self.n_nodes):
                sq = self.queues[i][0]
                while not sq.empty():
                    status = sq.get()
                    if status['state'] == 'LEADER':
                        print(f"Node {status['id']} became LEADER term {status['term']}")
                        leader_found = True
                        break
            if leader_found: break
            time.sleep(0.5)
        self.assertTrue(leader_found)

    def test_log_replication(self):
        """测试日志复制"""
        # 1. 等待Leader
        leader_id = -1
        while leader_id == -1:
            for i in range(self.n_nodes):
                sq = self.queues[i][0]
                while not sq.empty():
                    s = sq.get()
                    if s['state'] == 'LEADER': leader_id = s['id']
            time.sleep(0.5)
        
        # 2. 发送请求
        print(f"Sending request to Leader {leader_id}")
        self.queues[leader_id][1].put({'action': 'REQUEST'})
        
        # 3. 检查所有节点是否有日志
        time.sleep(2)
        logs_replicated = 0
        for i in range(self.n_nodes):
            sq = self.queues[i][0]
            last_status = None
            while not sq.empty(): last_status = sq.get()
            
            if last_status and len(last_status['log']) > 0:
                logs_replicated += 1
        
        self.assertGreaterEqual(logs_replicated, 2) # 至少大多数

if __name__ == '__main__':
    unittest.main()