from dataclasses import dataclass, field
from typing import List
import zmq
import zmq.asyncio
import asyncio
from config import *

global_context = zmq.asyncio.Context()

@dataclass
class LogEntry:
    cmd: str 
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
    socket: zmq.Socket

class MessageHandler:
    def __init__(self, my_id: int, peer_ids: List[int]):
        self.my_id = my_id
        
        self.server_sockets = {} 
        for pid in peer_ids:
            socket = global_context.socket(zmq.ROUTER)
            
            socket.setsockopt(zmq.LINGER, 0)
            
            port = get_dedicated_port(host_id=my_id, peer_id=pid)
            try:
                socket.bind(f"tcp://127.0.0.1:{port}")
                self.server_sockets[pid] = zmqInstance(socket)
            except zmq.ZMQError as e:
                print(f"Error binding port {port}: {e}")

        self.clients = {}
        for pid in peer_ids:
            socket = global_context.socket(zmq.DEALER)
            
            socket.setsockopt(zmq.LINGER, 0)
            
            socket.setsockopt_string(zmq.IDENTITY, str(my_id))
            
            target_port = get_dedicated_port(host_id=pid, peer_id=my_id)
            try:
                socket.connect(f"tcp://127.0.0.1:{target_port}")
                self.clients[pid] = zmqInstance(socket)
            except zmq.ZMQError as e:
                print(f"Error connecting port {target_port}: {e}")