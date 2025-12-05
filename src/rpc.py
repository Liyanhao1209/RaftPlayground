from dataclasses import dataclass, field
from typing import List

@dataclass
class LogEntry:
    cmd: str # simulation
    term: int
    index: int
    
@dataclass
class AppendEntriesRequest:
    term: int
    leaderId: int
    prevLogIndex: int
    prevLogTerm: int
    entries:List[LogEntry] = field(default_factory=List)
    leaderCommit:int 

@dataclass
class AppenEntriesResponse:
    term: int
    success: bool
    
@dataclass
class RequestVoteRequest:
    term: int
    candidateId: int
    lastLogIndex: int
    lastLogTerm: int
    
@dataclass
class RequestVoteResponse:
    term: int
    voteGranted: bool
