import unittest
import asyncio
import sys
import os
import time
import random

# --- 路径配置 ---
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

# --- Windows 环境兼容 ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import config
from server import Node, Role
from state import LogEntry

class TestRaftIntegration(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        # [策略 1] 为每个测试动态分配不同的端口段，彻底避开 "Address in use"
        # Test 1 -> 6001~6003, Test 2 -> 6011~6013, etc.
        # 获取当前测试方法的哈希或名称来区分有点麻烦，我们简单地通过计数器或者在每个 test 内部设置
        self.base_port = 6000 
        # 注意：具体的端口配置会在每个 test 方法内部被应用到 config.clusters
        self.nodes = []

    async def start_cluster(self, port_offset):
        """辅助函数：根据偏移量启动集群"""
        ports = {
            self.base_port + port_offset + 1: 1,
            self.base_port + port_offset + 2: 2,
            self.base_port + port_offset + 3: 3
        }
        
        # [核心] 原地更新 config.clusters，确保所有模块看到新配置
        config.clusters.clear()
        config.clusters.update(ports)
        
        print(f"Starting cluster on ports: {list(ports.keys())}")
        
        self.nodes = []
        for port in ports.keys():
            node = Node(port)
            self.nodes.append(node)
        
        for node in self.nodes:
            asyncio.create_task(node.start())
            
        await asyncio.sleep(1)

    async def asyncTearDown(self):
        print(f"[{self._testMethodName}] Tearing down & Cleaning sockets...")
        
        for node in self.nodes:
            # 1. 停止业务逻辑
            node.stop()
            
            # 2. [关键修复] 既然不能改源码，就在测试里手动帮它关 Socket！
            # 我们通过访问私有属性 msg_handler 来获取 socket 句柄
            if hasattr(node, 'msg_handler'):
                handler = node.msg_handler
                
                # 关闭 Server Sockets (Router)
                for zmq_inst in handler.server_sockets.values():
                    if not zmq_inst.socket.closed:
                        zmq_inst.socket.close()
                
                # 关闭 Client Sockets (Dealer)
                for zmq_inst in handler.clients.values():
                    if not zmq_inst.socket.closed:
                        zmq_inst.socket.close()
                
                # 清空引用
                handler.server_sockets.clear()
                handler.clients.clear()

        # 给 ZMQ 一点时间释放底层文件句柄
        await asyncio.sleep(0.5)

    async def get_leader(self) -> Node:
        for node in self.nodes:
            if not node.stopped and node.server.role == Role.Leader:
                return node
        return None

    async def wait_for_leader(self, timeout=5.0):
        start = time.time()
        while time.time() - start < timeout:
            leader = await self.get_leader()
            if leader:
                return leader
            await asyncio.sleep(0.2)
        return None

    # ================= 测试用例 =================

    async def test_1_leader_election(self):
        """测试用例 1: 基础 Leader 选举 (使用 6010 段端口)"""
        await self.start_cluster(port_offset=10)
        
        print("Waiting for leader election...")
        leader = await self.wait_for_leader(timeout=5.0)
        
        self.assertIsNotNone(leader, "Cluster failed to elect a leader within timeout")
        print(f"Leader elected: Node {leader.server.id}")
        
        current_term = leader.server.state.getCurrentTerm()
        leaders = 0
        for node in self.nodes:
            if not node.stopped and node.server.role == Role.Leader:
                if node.server.state.getCurrentTerm() == current_term:
                    leaders += 1
        
        self.assertEqual(leaders, 1, f"Found {leaders} leaders for term {current_term}, expected 1")

    async def test_2_log_replication(self):
        """测试用例 2: 日志复制 (使用 6020 段端口)"""
        # 使用不同的端口，防止 test_1 残留的 TIME_WAIT 影响
        await self.start_cluster(port_offset=20)
        
        leader = await self.wait_for_leader(timeout=5.0)
        self.assertIsNotNone(leader, "No leader for replication test")
        
        print(f"Leader is Node {leader.server.id}. Sending client request...")
        
        cmd_data = "TEST_CMD_1"
        term = leader.server.state.getCurrentTerm()
        
        # 安全获取 prev_idx
        logs = leader.server.state.getLog()
        prev_idx = logs[-1].index if logs else 0
        
        new_entry = LogEntry(cmd_data, term, prev_idx + 1)
        leader.server.appendLogEntry(new_entry)
        
        # 手动触发广播
        await leader.broadcast_heartbeat()
        await asyncio.sleep(1.0)
        
        replicated_count = 0
        for node in self.nodes:
            if node.stopped: continue
            logs = node.server.state.getLog()
            if not logs: continue
            
            last_entry = logs[-1]
            if last_entry.cmd == cmd_data and last_entry.index == new_entry.index:
                replicated_count += 1
        
        print(f"Log replicated to {replicated_count} nodes")
        self.assertGreaterEqual(replicated_count, 2, "Log replication failed to reach consensus")

    async def test_3_failover(self):
        """测试用例 3: 故障转移 (使用 6030 段端口)"""
        await self.start_cluster(port_offset=30)
        
        old_leader = await self.wait_for_leader(timeout=5.0)
        self.assertIsNotNone(old_leader, "Initial leader election failed")
        
        old_leader_id = old_leader.server.id
        old_term = old_leader.server.state.getCurrentTerm()
        print(f"Stopping Leader Node {old_leader_id} (Term {old_term})")
        
        old_leader.stop()
        
        print("Waiting for new election...")
        await asyncio.sleep(4.0) # 等待超时 + 选举
        
        new_leader = await self.get_leader()
        self.assertIsNotNone(new_leader, "Failover failed: No new leader elected")
        
        self.assertNotEqual(new_leader.server.id, old_leader_id, "Old leader is still marked as leader")
        
        new_term = new_leader.server.state.getCurrentTerm()
        print(f"New Leader: Node {new_leader.server.id} (Term {new_term})")
        self.assertGreater(new_term, old_term, "Term did not increment after election")

if __name__ == '__main__':
    unittest.main()