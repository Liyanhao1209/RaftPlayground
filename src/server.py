
from enum import Enum
from dataclasses import dataclass, field
from .state import State
from .rpc import *

class Role(Enum):
    Leader = 1
    Candidate = 2
    Follower = 3

@dataclass
class Server:
    role: Role = Role.Follower
    state: State
    id: int
    
    def update_term_vote(self,term:int):
        self.role = Role.Follower
        self.state.setCurrentTerm(term)
        self.state.setVoteFor(None)
        self.state.flushTerm(self.id)
        self.state.flushVote(self.id)
    
    def handle_vote_request(self,VoteRequest:RequestVoteRequest)->RequestVoteResponse:
        # Reply false if term < currentTerm
        server_term = self.state.getCurrentTerm()
        if server_term > VoteRequest.term:
            return RequestVoteResponse(server_term,False)
        
        if VoteRequest.term > server_term:
            self.update_term_vote(VoteRequest.term)
            server_term = VoteRequest.term
        
        # If votedFor is null or candidateId,
        # and candidate’s log is at least as up-to-date as receiver’s log,
        # grant vote
        voteFor = self.state.getVoteId()
        logs = self.state.getLog()
        vote_available = voteFor is None or voteFor == VoteRequest.candidateId
        log_up2date = not len(logs) or logs[-1].term < VoteRequest.lastLogTerm or logs[-1].term == VoteRequest.lastLogTerm and logs[-1].index <= VoteRequest.lastLogIndex
        
        if vote_available and log_up2date:
            self.state.setVoteFor(VoteRequest.candidateId)
            self.state.flushVote(self.id)
            return RequestVoteResponse(server_term,True)
        
        return RequestVoteResponse(server_term,False)
    
    def handle_append_entries_request(self,AppendEntriesRequest:AppendEntriesRequest)->AppenEntriesResponse:
        # Reply false if term < currentTerm
        server_term = self.state.getCurrentTerm()
        if server_term > AppendEntriesRequest.term:
            return AppenEntriesResponse(server_term,False)
        
        if AppendEntriesRequest.term > server_term:
            self.update_term_vote(AppendEntriesRequest.term)
            server_term = AppendEntriesRequest.term
        
        # Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        logs = self.state.getLog()
        if logs[-1].index < AppendEntriesRequest.prevLogIndex or logs[AppendEntriesRequest.prevLogIndex].term != AppendEntriesRequest.prevLogTerm:
            return AppenEntriesResponse(server_term,False)
        
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
        
        if AppendEntriesRequest.leaderCommit > self.state.getCommitId():
            self.state.setCommitId(
                min(AppendEntriesRequest.leaderCommit,AppendEntriesRequest.entries[-1].index)
            )
        
        return AppenEntriesResponse(server_term,True)

        