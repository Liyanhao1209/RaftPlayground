from dataclasses import dataclass, field, asdict
from typing import List,Optional
from .rpc import *
from .config import *

import json,os

@dataclass
class StableState:
    currentTerm: int
    votedFor: Optional[int]
    log: List[LogEntry] = field(default_factory=lambda:[LogEntry('dummy',-1,0)])
    
    # for recovery
    def flushLog(self,ServerId:int):
        state_pth = os.path.join(DATAPTH,f"{ServerId}_log_state.json")
        with open(state_pth,'w',encoding='utf-8') as sf:
            data = {
                "log": [asdict(l) for l in self.log]
            }
            
            json.dump(data,sf)
            
    def flushTerm(self,ServerId:int):
        state_pth = os.path.join(DATAPTH,f"{ServerId}_term_state.json")
        with open(state_pth,'w',encoding='utf-8') as sf:
            data = {
                "currentTerm": self.currentTerm
            }
            
            json.dump(data,sf)
            
    def flushVote(self,ServerId:int):
        state_pth = os.path.join(DATAPTH,f"{ServerId}_vote_state.json")
        with open(state_pth,'w',encoding='utf-8') as sf:
            data = {
                "votedFor": self.votedFor
            }
            
            json.dump(data,sf)
        
@dataclass
class ServerState:
    commitIndex: int
    lastApplied: int

@dataclass
class LeaderState:
    nextIndex: dict[int,int]
    matchIndex: dict[int,int]
    
@dataclass
class State:
    stable: StableState
    server: ServerState
    leader: Optional[LeaderState]
    
    def getCurrentTerm(self)->int:
        return self.stable.currentTerm
    
    def getVoteId(self)->Optional[int]:
        return self.stable.votedFor
    
    def getLog(self)->List[LogEntry]:
        return self.stable.log
    
    def getCommitId(self)->int:
        return self.server.commitIndex
    
    def setCurrentTerm(self,newTerm:int):
        self.stable.currentTerm = newTerm
    
    def setVoteFor(self,voteId:Optional[int]):
        self.stable.votedFor = voteId
        
    def setLogs(self,logs:List[LogEntry]):
        self.stable.log = logs
        
    def setCommitId(self,newId):
        self.server.commitIndex = newId
    
    def grantVote(self,VoteRequest:RequestVoteRequest)->RequestVoteResponse:
        return self.stable.grantVote(VoteRequest)
    
    def flushLog(self,ServerId:int):
        self.stable.flushLog(ServerId)
        
    def flushTerm(self,ServerId:int):
        self.stable.flushTerm(ServerId)
    
    def flushVote(self,ServerId:int):
        self.stable.flushVote(ServerId)   
        
        