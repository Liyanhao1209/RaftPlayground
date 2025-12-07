from dataclasses import dataclass, field
from typing import List
import zmq,zmq.asyncio,asyncio
from .config import *

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
    leaderCommit:int
    entries:List[LogEntry] = field(default_factory=list)

@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    followerId: int
    matchIndex: int
    
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

@dataclass
class zmqInstance:
    context: zmq.asyncio.Context
    socket: zmq.Socket

def singleServer(port:str):
    context = zmq.asyncio.Context()
    socket = context.socket(zmq.ROUTER)
    socket.bind(f"tcp://127.0.0.1:{port}")
    
    return zmqInstance(context,socket)

def multiClients(other_ports:List[str]):
    m = {}
    for i,port in enumerate(other_ports):
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.DEALER)
        socket.connect(f"tcp://127.0.0.1:{port}")
        m[int(port)] = zmqInstance(context,socket)

    return m
    
class MessageHandler:
    def __init__(self, my_id: int, peer_ids: List[int]):
        self.my_id = my_id
        
        self.server_sockets = {} 
        for pid in peer_ids:
            context = zmq.asyncio.Context()
            socket = context.socket(zmq.ROUTER)
            
            port = get_dedicated_port(host_id=my_id, peer_id=pid)
            socket.bind(f"tcp://127.0.0.1:{port}")
            
            self.server_sockets[pid] = zmqInstance(context, socket)

        self.clients = {}
        for pid in peer_ids:
            context = zmq.asyncio.Context()
            socket = context.socket(zmq.DEALER)
            
            socket.setsockopt_string(zmq.IDENTITY, str(my_id))
            
            target_port = get_dedicated_port(host_id=pid, peer_id=my_id)
            socket.connect(f"tcp://127.0.0.1:{target_port}")
            
            self.clients[pid] = zmqInstance(context, socket)