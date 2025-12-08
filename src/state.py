from dataclasses import dataclass, field, asdict
from typing import List, Optional
from rpc import *

@dataclass
class StableState:
    currentTerm: int
    votedFor: Optional[int]
    log: List[LogEntry] = field(default_factory=lambda: [LogEntry('dummy', -1, 0)])
    
    def flushLog(self, ServerId: int): pass
    def flushTerm(self, ServerId: int): pass
    def flushVote(self, ServerId: int): pass

@dataclass
class ServerState:
    commitIndex: int
    lastApplied: int

@dataclass
class LeaderState:
    nextIndex: dict[int, int]
    matchIndex: dict[int, int]

@dataclass
class State:
    stable: StableState
    server: ServerState
    leader: Optional[LeaderState]

    def getCurrentTerm(self) -> int: return self.stable.currentTerm
    def getVoteId(self) -> Optional[int]: return self.stable.votedFor
    def getLog(self) -> List[LogEntry]: return self.stable.log
    def getCommitId(self) -> int: return self.server.commitIndex
    
    def setCurrentTerm(self, newTerm: int): self.stable.currentTerm = newTerm
    def setVoteFor(self, voteId: Optional[int]): self.stable.votedFor = voteId
    def setLogs(self, logs: List[LogEntry]): self.stable.log = logs
    def setCommitId(self, newId): self.server.commitIndex = newId
    
    def flushLog(self, ServerId: int): pass
    def flushTerm(self, ServerId: int): pass
    def flushVote(self, ServerId: int): pass