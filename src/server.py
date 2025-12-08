from enum import Enum
from typing import Optional
from dataclasses import dataclass, field
from state import *
from rpc import *
from config import *
import pickle
import asyncio

class Role(Enum):
    Leader = 1
    Candidate = 2
    Follower = 3

@dataclass
class Server:
    role: Role
    state: State
    id: int
    
    def update_term_vote(self, term: int, vote: Optional[int] = None):
        self.state.setCurrentTerm(term)
        self.state.setVoteFor(vote)
        self.state.flushTerm(self.id)
        self.state.flushVote(self.id)
    
    def handle_vote_request(self, VoteRequest: RequestVoteRequest) -> RequestVoteResponse:
        server_term = self.state.getCurrentTerm()
        
        # 1. Reply false if term < currentTerm
        if server_term > VoteRequest.term:
            return RequestVoteResponse(server_term, False)
        
        # Update term if needed
        if VoteRequest.term > server_term:
            self.role = Role.Follower
            self.update_term_vote(VoteRequest.term)
            server_term = VoteRequest.term
        
        # 2. Grant vote if votedFor is null/candidateId AND log is up-to-date
        voteFor = self.state.getVoteId()
        logs = self.state.getLog()
        
        vote_available = (voteFor is None or voteFor == VoteRequest.candidateId)
        
        # Log completeness check:
        # Raft restriction: candidate's log must be at least as up-to-date as receiver's.
        # Up-to-date means: later term, or same term and longer index.
        last_log_term = logs[-1].term
        last_log_index = logs[-1].index
        
        log_is_ok = (VoteRequest.lastLogTerm > last_log_term) or \
                    (VoteRequest.lastLogTerm == last_log_term and VoteRequest.lastLogIndex >= last_log_index)
        
        if vote_available and log_is_ok:
            self.state.setVoteFor(VoteRequest.candidateId)
            self.state.flushVote(self.id)
            return RequestVoteResponse(server_term, True)
        
        return RequestVoteResponse(server_term, False)
    
    def handle_append_entries_request(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        server_term = self.state.getCurrentTerm()
        
        # 1. Reply false if term < currentTerm
        if server_term > req.term:
            return AppendEntriesResponse(server_term, False, self.id, 0)
        
        # Update term
        if req.term > server_term:
            self.role = Role.Follower
            self.update_term_vote(req.term)
            server_term = req.term
        
        # 2. Consistency Check
        logs = self.state.getLog()
        
        # Check if prevLogIndex is within bounds
        if req.prevLogIndex >= len(logs):
             return AppendEntriesResponse(server_term, False, self.id, 0)
         
        # Check term match at prevLogIndex
        if logs[req.prevLogIndex].term != req.prevLogTerm:
            return AppendEntriesResponse(server_term, False, self.id, 0)
        
        # 3. Append new entries
        new_entries = req.entries
        match_index = req.prevLogIndex # 默认为 prevLogIndex (用于心跳)
        
        if new_entries:
            for i, entry in enumerate(new_entries):
                idx = entry.index
                match_index = idx # 更新匹配到的最新 index
                
                # If new entry goes beyond current log, just append
                if idx >= len(logs):
                    logs = logs + new_entries[i:]
                    break
                
                # If conflict, truncate and append
                if logs[idx].term != entry.term:
                    logs = logs[:idx] + new_entries[i:]
                    break
            
            self.state.setLogs(logs)
            self.state.flushLog(self.id)
        
        # 4. Update Commit Index
        last_new_index = req.prevLogIndex
        if req.entries:
            last_new_index = req.entries[-1].index

        if req.leaderCommit > self.state.getCommitId():
            self.state.setCommitId(
                min(req.leaderCommit, last_new_index)
            )
            
        return AppendEntriesResponse(server_term, True, self.id, last_new_index)
    
    def handle_append_entries_response(self, resp: AppendEntriesResponse) -> Optional[AppendEntriesRequest]:
        if self.role != Role.Leader:
            return None
        
        # If response term is higher, step down
        if resp.term > self.state.getCurrentTerm():
            self.role = Role.Follower
            self.update_term_vote(resp.term)
            return None
        
        if resp.term < self.state.getCurrentTerm():
            return None
        
        leader_state = self.state.leader
        # [Safety Check] Ensure leader state is initialized
        if not leader_state:
            return None
        
        if not resp.success:
            # Backtrack nextIndex
            curr_next = leader_state.nextIndex.get(resp.followerId, 1)
            leader_state.nextIndex[resp.followerId] = max(1, curr_next - 1)
            
            return self.genAppendEntriesRequest(resp.followerId)
        else:
            # Update matchIndex and nextIndex
            match_index = resp.matchIndex
            current_match = leader_state.matchIndex.get(resp.followerId, 0)
            
            if match_index > current_match:
                leader_state.matchIndex[resp.followerId] = match_index
                leader_state.nextIndex[resp.followerId] = match_index + 1
            
            self.updateCommitIndex()
            return None
        
    def updateCommitIndex(self):
        if not self.state.leader: return

        # Get all match indexes plus our own last index
        match_indexes = list(self.state.leader.matchIndex.values())
        my_last_log_index = self.state.getLog()[-1].index
        match_indexes.append(my_last_log_index)
        match_indexes.sort()
        
        # Find median (majority commit)
        n_peers = len(match_indexes)
        majority_idx = n_peers // 2
        N = match_indexes[majority_idx]
        
        # Check if we can commit N
        if N > self.state.getCommitId():
            logs = self.state.getLog()
            # Find entry at N (since logs might be truncated/snapshot in real raft, but here list access is tricky if N is small)
            # logs index matches logic index only if no compaction. 
            # Here logs[0] is dummy (index 0). So logs[N] is entry with index N.
            if N < len(logs) and logs[N].term == self.state.getCurrentTerm():
                self.state.setCommitId(N)
    
    def genElectionRequest(self) -> RequestVoteRequest:
        nterm = self.state.getCurrentTerm() + 1
        self.state.setCurrentTerm(nterm)
        self.state.setVoteFor(self.id)
        self.update_term_vote(nterm, self.id)
        
        logs = self.state.getLog()
        return RequestVoteRequest(nterm, self.id, logs[-1].index, logs[-1].term)
    
    def genAppendEntriesRequest(self, follower_id) -> AppendEntriesRequest:
        # [Safety Check]
        if self.role != Role.Leader or not self.state.leader:
            raise Exception("Generating AppendEntries but not leader or state not init")

        term = self.state.getCurrentTerm()
        logs = self.state.getLog()
        
        next_idx = self.state.leader.nextIndex.get(follower_id, 1)
        prevIndex = next_idx - 1
        
        # Safety: prevIndex should always be valid since logs has dummy at 0
        prevTerm = logs[prevIndex].term
        
        entries = logs[next_idx:]
        leaderCommit = self.state.getCommitId()
        
        return AppendEntriesRequest(term, self.id, prevIndex, prevTerm, leaderCommit, entries)

    def appendLogEntry(self, entry: LogEntry):
        logs = self.state.getLog()
        logs.append(entry)
        self.state.setLogs(logs)
    
class Node:
    def __init__(self, port: str):
        self.server = Server(
            Role.Follower,
            State(
                 stable=StableState(0, None, [LogEntry('dummy', -1, 0)]),
                 server=ServerState(0, 0),
                 leader=None
            ),
            clusters[port]
        )
        
        my_id = clusters[port]
        peer_ids = [clusters[p] for p in clusters.keys() if p != port]
        
        self.msg_handler = MessageHandler(my_id, peer_ids)
        
        self.votes_received = 0
        self.running = True
        self.stopped = False 
        
        self.election_timer_task = None 
        self.heartbeat_loop_task = None
    
    async def become_leader(self):
        self.server.role = Role.Leader
        
        last_log_idx = self.server.state.getLog()[-1].index
             
        follower_ids = list(self.msg_handler.clients.keys())
        
        # [Fix 2] Assign to 'leader' attribute, NOT 'LeaderState'
        self.server.state.leader = LeaderState(
            nextIndex={fid: last_log_idx + 1 for fid in follower_ids},
            matchIndex={fid: 0 for fid in follower_ids}
        )
        
        if self.election_timer_task:
            self.election_timer_task.cancel()
            self.election_timer_task = None
            
        await self.broadcast_heartbeat()
        self.heartbeat_loop_task = asyncio.create_task(self.run_heartbeat_loop())

    async def become_follower(self, term):
        self.server.role = Role.Follower
        self.server.update_term_vote(term, None)
        
        # Clear leader state
        self.server.state.leader = None

        if self.heartbeat_loop_task:
            self.heartbeat_loop_task.cancel()
            self.heartbeat_loop_task = None
            
        self.reset_election_timer()

    async def run_heartbeat_loop(self):
        try:
            while self.server.role == Role.Leader:
                await self.broadcast_heartbeat()
                await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            pass

    async def broadcast_heartbeat(self):
        if not self.server.state.leader: return
        
        for peer_id, client in self.msg_handler.clients.items():
            try:
                req = self.server.genAppendEntriesRequest(peer_id)
                await client.socket.send(pickle.dumps(req))
            except Exception as e:
                print(f"Error broadcasting to {peer_id}: {e}")
    
    async def start(self):
        for peer_id, zmq_inst in self.msg_handler.server_sockets.items():
            asyncio.create_task(self.recv_loop_for_peer(peer_id, zmq_inst.socket))
            
        for peer_id, zmq_inst in self.msg_handler.clients.items():
            asyncio.create_task(self.recv_loop_for_peer(peer_id, zmq_inst.socket))
        
        self.reset_election_timer()

        while self.running:
            await asyncio.sleep(1)

    async def recv_loop_for_peer(self, peer_id, socket):
        # print(f"Node {self.server.id}: Listening for peer {peer_id}...") 
        
        while self.running:
            try:
                frames = await socket.recv_multipart()
                
                content = frames[-1] 
                rpc_msg = pickle.loads(content)
                
                print(f"Node {self.server.id} RECV from {peer_id}: {rpc_msg}")
                
                await self.handle_message(rpc_msg, sender_id=peer_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Recv error at Node {self.server.id}: {e}")
                await asyncio.sleep(0.1)

    async def handle_message(self, msg, sender_id):
        if self.stopped:
            return

        async def reply(payload):
            if sender_id in self.msg_handler.server_sockets:
                zmq_inst = self.msg_handler.server_sockets[sender_id]
                target_identity = str(sender_id).encode('utf-8')
                await zmq_inst.socket.send_multipart([target_identity, pickle.dumps(payload)])

        current_term = self.server.state.getCurrentTerm()
        
        # Universal rule: if RPC term > currentTerm, become follower
        if hasattr(msg, 'term') and msg.term > current_term:
            await self.become_follower(msg.term)
        
        if isinstance(msg, AppendEntriesRequest):
            resp = self.server.handle_append_entries_request(msg)
            await reply(resp)
            
            # Only reset timer if the leader is valid (term >= currentTerm)
            if msg.term >= self.server.state.getCurrentTerm():
                 self.reset_election_timer()

        elif isinstance(msg, RequestVoteRequest):
            resp = self.server.handle_vote_request(msg)
            print(f"{self.server.id} handle {msg}:{resp}")
            await reply(resp)
            
            if resp.voteGranted:
                self.reset_election_timer()

        elif isinstance(msg, RequestVoteResponse):
            if self.server.role == Role.Candidate:
                if msg.term == self.server.state.getCurrentTerm() and msg.voteGranted:
                    self.votes_received += 1
                    total_nodes = len(clusters) 
                    if self.votes_received > total_nodes // 2:
                        await self.become_leader()
        
        elif isinstance(msg, AppendEntriesResponse):
             retry_req = self.server.handle_append_entries_response(msg)
             if retry_req:
                 if sender_id in self.msg_handler.clients:
                     client = self.msg_handler.clients[sender_id]
                     await client.socket.send(pickle.dumps(retry_req))

    def reset_election_timer(self):
        if self.election_timer_task:
            self.election_timer_task.cancel()
        
        # Increased timeout slightly for stability in visualizer
        timeout = gen_timeout(0.8,5) 
        self.election_timer_task = asyncio.create_task(self.wait_for_election(timeout))

    async def wait_for_election(self, timeout):
        try:
            await asyncio.sleep(timeout)
            print(f"Node {self.server.id}: Election Timeout! Become Candidate.")
            await self.start_election()
        except asyncio.CancelledError:
            pass

    async def start_election(self):
        self.server.role = Role.Candidate
        req = self.server.genElectionRequest() 
        self.votes_received = 1 
        
        for peer_id, client_inst in self.msg_handler.clients.items():
            print(f"send vote request to {peer_id} from {self.server.id}")
            await client_inst.socket.send(pickle.dumps(req))
        
        self.reset_election_timer()
        
    # --- Visualizer Control Methods (Fixed variable names) ---
    def stop(self):
        self.stopped = True
        if self.election_timer_task: self.election_timer_task.cancel()
        if self.heartbeat_loop_task: self.heartbeat_loop_task.cancel()
        print(f"Node {self.server.id} STOPPED")
        
    def resume(self):
        if not self.stopped: return
        self.stopped = False
        self.server.role = Role.Follower 
        self.reset_election_timer()
        print(f"Node {self.server.id} RESUMED")
        
    def force_timeout(self):
        if self.stopped: return
        if self.election_timer_task: self.election_timer_task.cancel()
        asyncio.create_task(self.start_election())
        print(f"Node {self.server.id} FORCE TIMEOUT")
        
    def client_request(self):
        if self.stopped: return
        if self.server.role != Role.Leader:
            print(f"Node {self.server.id} is not Leader, ignoring request.")
            return
        
        import random
        cmd = f"C{random.randint(10, 99)}"
        entry = LogEntry(cmd, self.server.state.getCurrentTerm(), self.server.state.getLog()[-1].index + 1)
        self.server.appendLogEntry(entry)
        print(f"Node {self.server.id} Client Request: {cmd}")
        asyncio.create_task(self.broadcast_heartbeat())