
from enum import Enum
from typing import Optional
from dataclasses import dataclass, field
from .state import *
from .rpc import *
from .config import *
import pickle

class Role(Enum):
    Leader = 1
    Candidate = 2
    Follower = 3

@dataclass
class Server:
    role: Role = Role.Follower
    state: State
    id: int
    
    def update_term_vote(self,term:int,vote:Optional[int]=None):
        self.state.setCurrentTerm(term)
        self.state.setVoteFor(vote)
        self.state.flushTerm(self.id)
        self.state.flushVote(self.id)
    
    def handle_vote_request(self,VoteRequest:RequestVoteRequest)->RequestVoteResponse:
        # Reply false if term < currentTerm
        server_term = self.state.getCurrentTerm()
        if server_term > VoteRequest.term:
            return RequestVoteResponse(server_term,False)
        
        if VoteRequest.term > server_term:
            self.role = Role.Follower
            self.update_term_vote(VoteRequest.term)
            server_term = VoteRequest.term
        
        # If votedFor is null or candidateId,
        # and candidate’s log is at least as up-to-date as receiver’s log,
        # grant vote
        
        # election restriction
        voteFor = self.state.getVoteId()
        logs = self.state.getLog()
        vote_available = voteFor is None or voteFor == VoteRequest.candidateId
        log_up2date = not len(logs) or logs[-1].term < VoteRequest.lastLogTerm or logs[-1].term == VoteRequest.lastLogTerm and logs[-1].index <= VoteRequest.lastLogIndex
        
        if vote_available and log_up2date:
            self.state.setVoteFor(VoteRequest.candidateId)
            self.state.flushVote(self.id)
            return RequestVoteResponse(server_term,True)
        
        return RequestVoteResponse(server_term,False)
    
    def handle_append_entries_request(self,AppendEntriesRequest:AppendEntriesRequest)->AppendEntriesResponse:
        # Reply false if term < currentTerm
        server_term = self.state.getCurrentTerm()
        if server_term > AppendEntriesRequest.term:
            return AppendEntriesResponse(server_term,False,self.id,0)
        
        if AppendEntriesRequest.term > server_term:
            self.role = Role.Follower
            self.update_term_vote(AppendEntriesRequest.term)
            server_term = AppendEntriesRequest.term
        
        # Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        logs = self.state.getLog()
        
        if AppendEntriesRequest.prevLogIndex >= len(logs):
             return AppendEntriesResponse(server_term, False, self.id, 0)
         
        if logs[AppendEntriesRequest.prevLogIndex].term != AppendEntriesRequest.prevLogTerm:
            return AppendEntriesResponse(server_term, False, self.id, 0)
        # if logs[-1].index < AppendEntriesRequest.prevLogIndex or logs[AppendEntriesRequest.prevLogIndex].term != AppendEntriesRequest.prevLogTerm:
        #     return AppendEntriesResponse(server_term,False,self.id,0)
        
        new_entries = AppendEntriesRequest.entries
        if new_entries:
            for i, entry in enumerate(new_entries):
                idx = entry.index
                
                if idx >= len(logs):
                    logs = logs + new_entries[i:]
                    break
                
                if logs[idx].term != entry.term:
                    logs = logs[:idx] + new_entries[i:]
                    break
            
            self.state.setLogs(logs)
            self.state.flushLog(self.id)
        
        last_new_index = AppendEntriesRequest.prevLogIndex
        if AppendEntriesRequest.entries:
            last_new_index = AppendEntriesRequest.entries[-1].index

        if AppendEntriesRequest.leaderCommit > self.state.getCommitId():
            self.state.setCommitId(
                min(AppendEntriesRequest.leaderCommit, last_new_index)
            )
        
        return AppendEntriesResponse(server_term,True,self.id,logs[-1].index)
    
    def handle_append_entries_response(self,AppendEntriesResponse:AppendEntriesResponse)->Optional[AppendEntriesRequest]:
        if self.role != Role.Leader:
            return None
        
        if AppendEntriesResponse.term > self.state.getCurrentTerm():
            self.role = Role.Follower
            self.update_term_vote(AppendEntriesResponse.term)
            return None
        
        if AppendEntriesResponse.term < self.state.getCurrentTerm():
            return None
        
        leader_state = self.state.leader
        
        if not AppendEntriesResponse.success:
            # traceback to the first inconsistent entry
            curr_next = leader_state.nextIndex[AppendEntriesResponse.followerId]
            leader_state.nextIndex[AppendEntriesResponse.followerId] = max(1,curr_next-1)
            
            return self.genAppendEntriesRequest(AppendEntriesResponse.followerId)
        else:
            # update nextIndex && matchIndex && commitIndex
            followerId = AppendEntriesResponse.followerId
            match_index = AppendEntriesResponse.matchIndex
            if match_index > self.state.leader.matchIndex[followerId]:
                self.state.leader.matchIndex[followerId] = match_index
                self.state.leader.nextIndex[followerId] = match_index + 1
            
            self.updateCommitIndex()
            
            return None
        
    def updateCommitIndex(self):
        # try to update commit index
        match_indexes = list(self.state.leader.matchIndex.values())
        
        my_last_log_index = self.state.getLog()[-1].index
        match_indexes.append(my_last_log_index)
        
        match_indexes.sort()
        
        # find the median
        n_peers = len(match_indexes)
        majority_idx = n_peers // 2
        
        N = match_indexes[majority_idx]
        
        # leader can only submit entries from its own term (and the previous ones)
        logs = self.state.getLog()
        if N > self.state.getCommitId():
            entry_at_N = None
            for entry in logs:
                if entry.index == N:
                    entry_at_N = entry
                    break
            
            if entry_at_N and entry_at_N.term == self.state.getCurrentTerm():
                self.state.setCommitId(N)
    
    def genElectionRequest(self)->RequestVoteRequest:
        # increment currentTerm
        nterm = self.state.getCurrentTerm()+1
        self.state.setCurrentTerm(nterm)
        
        # vote for self
        self.state.setVoteFor(self.id)
        
        self.update_term_vote(nterm,self.id)
        
        logs = self.state.getLog()
        return RequestVoteRequest(nterm,self.id,logs[-1].index,logs[-1].term)
    
    def genAppendEntriesRequest(self,follower_id)->AppendEntriesRequest:
        assert(self.role == Role.Leader)
        
        term = self.state.getCurrentTerm()
        logs = self.state.getLog()
        next_idx = self.state.leader.nextIndex[follower_id]
        prevIndex = next_idx - 1
        prevTerm = logs[prevIndex].term
        leaderCommit = self.state.getCommitId()
        
        return AppendEntriesRequest(term,self.id,prevIndex,prevTerm,logs[next_idx:],leaderCommit)

    def appendLogEntry(self,entry:LogEntry):
        logs = self.state.getLog()
        logs.append(entry)
        
        self.state.setLogs(logs)
    
class Node:
    def __init__(self,port:str):
        self.server = Server(Role.Follower,
                             State(
                                 stable= StableState(0,None,[LogEntry('dummy',-1,0)]),
                                 server= ServerState(0,0),
                                 leader= None
                                ),
                             clusters[port]
                            )
        self.msg_handler = MessageHandler(port,[p for p in clusters.keys() if p!=port])
        
        self.votes_received = 0 # for vote
        self.running = True
        self.election_timer_task = None # Follower/Candidate 
        self.heartbeat_loop_task = None # Leader
    
    async def become_leader(self):
        self.server.role = Role.Leader
        
        last_log_idx = 0
        logs = self.server.state.getLog()
        if logs:
             last_log_idx = logs[-1].index
             
        follower_ids = list(self.msg_handler.clients.keys())
        
        self.server.state.LeaderState = LeaderState(
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
        
        for peer_id, client in self.msg_handler.clients.items():
            asyncio.create_task(client.socket.send_pyobj(self.server.genAppendEntriesRequest(peer_id)))
            # await client.socket.send_pyobj(req)
    
    async def start(self):
        for peer_id, zmq_inst in self.msg_handler.server_sockets.items():
            asyncio.create_task(self.recv_loop_for_peer(peer_id, zmq_inst.socket))
        
        self.reset_election_timer()

        while self.running:
            await asyncio.sleep(1)

    async def recv_loop_for_peer(self, peer_id, socket):
        while self.running:
            frames = await socket.recv_multipart()
            
            content = frames[-1] 
            rpc_msg = pickle.loads(content)
            
            await self.handle_message(rpc_msg, sender_id=peer_id)
    
    async def recv_loop(self):
        mysock = self.msg_handler.server.socket
        while self.running:
            frames = await mysock.recv_multipart()
            sender_id = frames[0] 
            content = frames[-1] 
            rpc_msg = pickle.loads(content)
            
            await self.handle_message(rpc_msg, sender_id)

    async def handle_message(self, msg, sender_id):
        def reply(payload):
            zmq_inst = self.msg_handler.server_sockets[sender_id]
            socket = zmq_inst.socket
            
            target_identity = str(sender_id).encode('utf-8')
            
            socket.send_multipart([target_identity, pickle.dumps(payload)])

        current_term = self.server.state.getCurrentTerm()
        
        if hasattr(msg, 'term') and msg.term > current_term:
            await self.become_follower(msg.term)
        
        if isinstance(msg, AppendEntriesRequest):
            resp = self.server.handle_append_entries_request(msg)
            reply(resp)
            
            if msg.term == self.server.state.getCurrentTerm():
                 self.reset_election_timer()

        elif isinstance(msg, RequestVoteRequest):
            resp = self.server.handle_vote_request(msg)
            reply(resp)


        elif isinstance(msg, RequestVoteResponse):
            if self.server.role == Role.Candidate:
                if msg.term == self.server.state.getCurrentTerm() and msg.voteGranted:
                    self.votes_received += 1
                    print(f"Node {self.server.id} received vote. Total: {self.votes_received}")
                    
                    total_nodes = len(clusters) 
                    if self.votes_received > total_nodes // 2:
                        await self.become_leader()

        elif isinstance(msg, AppendEntriesResponse):
             retry_req = self.server.handle_append_entries_response(msg)
             if retry_req:
                 self.msg_handler.server.socket.send_multipart([sender_id, pickle.dumps(retry_req)])

    def reset_election_timer(self):
        if self.election_timer_task:
            self.election_timer_task.cancel()
        
        timeout = gen_timeout(1.5,3)
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
            asyncio.create_task(client_inst.socket.send(pickle.dumps(req)))
        
        self.reset_election_timer()
