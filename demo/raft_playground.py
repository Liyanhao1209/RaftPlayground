import sys
import math
import random
import copy
from enum import Enum
from PyQt5.QtWidgets import (QApplication, QMainWindow, QGraphicsScene, QGraphicsView, 
                             QGraphicsItem, QSlider, QVBoxLayout, QWidget, QHBoxLayout, 
                             QLabel, QPushButton, QMenu, QAction)
from PyQt5.QtCore import Qt, QTimer, QRectF, QPointF
from PyQt5.QtGui import QPainter, QPen, QBrush, QColor, QFont

# ==========================================
# 1. 配置与常量
# ==========================================

NUM_SERVERS = 5
RPC_TIMEOUT = 50000.0
MIN_RPC_LATENCY = 10000.0
MAX_RPC_LATENCY = 15000.0
ELECTION_TIMEOUT = 100000.0
BATCH_SIZE = 1

# 配色方案 (对应原版 termColors)
TERM_COLORS = ['#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3', '#a6d854', '#ffd92f']

def make_election_alarm(now):
    return now + (random.random() + 1.0) * ELECTION_TIMEOUT

def circle_coord(frac, cx, cy, r):
    radians = 2 * math.pi * (0.75 + frac)
    return {'x': cx + r * math.cos(radians), 'y': cy + r * math.sin(radians)}

# ==========================================
# 2. 核心数据模型
# ==========================================

class MessageType(Enum):
    RequestVote = "RequestVote"
    AppendEntries = "AppendEntries"

class Direction(Enum):
    Request = "request"
    Reply = "reply"

class ServerState(Enum):
    Follower = "follower"
    Candidate = "candidate"
    Leader = "leader"
    Stopped = "stopped"

class LogEntry:
    def __init__(self, term, value):
        self.term = term
        self.value = value

class Message:
    def __init__(self):
        self.from_id = 0
        self.to_id = 0
        self.type = None
        self.direction = None
        self.send_time = 0
        self.recv_time = 0
        self.payload = {}
    
    def clone(self):
        m = Message()
        m.from_id = self.from_id
        m.to_id = self.to_id
        m.type = self.type
        m.direction = self.direction
        m.send_time = self.send_time
        m.recv_time = self.recv_time
        m.payload = copy.deepcopy(self.payload)
        return m

class Server:
    def __init__(self, server_id, peers):
        self.id = server_id
        self.peers = peers
        self.state = ServerState.Follower
        self.term = 1
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.election_alarm = make_election_alarm(0)
        
        self.vote_granted = {p: False for p in peers}
        self.match_index = {p: 0 for p in peers}
        self.next_index = {p: 1 for p in peers}
        self.rpc_due = {p: 0 for p in peers}
        self.heartbeat_due = {p: 0 for p in peers}

    def clone(self):
        s = Server(self.id, self.peers)
        s.state = self.state
        s.term = self.term
        s.voted_for = self.voted_for
        s.log = [LogEntry(e.term, e.value) for e in self.log]
        s.commit_index = self.commit_index
        s.election_alarm = self.election_alarm
        s.vote_granted = self.vote_granted.copy()
        s.match_index = self.match_index.copy()
        s.next_index = self.next_index.copy()
        s.rpc_due = self.rpc_due.copy()
        s.heartbeat_due = self.heartbeat_due.copy()
        return s

class RaftModel:
    def __init__(self):
        self.time = 0.0
        self.servers = []
        self.messages = []
        for i in range(1, NUM_SERVERS + 1):
            peers = [j for j in range(1, NUM_SERVERS + 1) if j != i]
            self.servers.append(Server(i, peers))

    def clone(self):
        m = RaftModel()
        m.time = self.time
        m.servers = [s.clone() for s in self.servers]
        m.messages = [msg.clone() for msg in self.messages]
        return m

# ==========================================
# 3. 核心逻辑 (Raft Logic)
# ==========================================

def log_term(log, index):
    if index < 1 or index > len(log): return 0
    return log[index - 1].term

def step_down(model, server, term):
    server.term = term
    server.state = ServerState.Follower
    server.voted_for = None
    if server.election_alarm <= model.time or server.election_alarm == float('inf'):
        server.election_alarm = make_election_alarm(model.time)

def send_message(model, msg):
    msg.send_time = model.time
    msg.recv_time = model.time + MIN_RPC_LATENCY + random.random() * (MAX_RPC_LATENCY - MIN_RPC_LATENCY)
    model.messages.append(msg)

# --- Rules ---

def rules_start_new_election(model, server):
    # 如果超时，或者是 Follower/Candidate 且时间到了
    if (server.state in [ServerState.Follower, ServerState.Candidate] and 
        server.election_alarm <= model.time):
        server.election_alarm = make_election_alarm(model.time)
        server.term += 1
        server.voted_for = server.id
        server.state = ServerState.Candidate
        server.vote_granted = {p: False for p in server.peers}
        server.match_index = {p: 0 for p in server.peers}
        server.next_index = {p: 1 for p in server.peers}
        server.rpc_due = {p: 0 for p in server.peers}
        server.heartbeat_due = {p: 0 for p in server.peers}

def rules_become_leader(model, server):
    if server.state == ServerState.Candidate:
        votes = sum(1 for p in server.peers if server.vote_granted[p]) + 1
        if votes > NUM_SERVERS / 2:
            server.state = ServerState.Leader
            server.next_index = {p: len(server.log) + 1 for p in server.peers}
            server.rpc_due = {p: float('inf') for p in server.peers}
            server.heartbeat_due = {p: 0 for p in server.peers}
            server.election_alarm = float('inf')

def rules_send_request_vote(model, server, peer):
    if server.state == ServerState.Candidate and server.rpc_due[peer] <= model.time:
        server.rpc_due[peer] = model.time + RPC_TIMEOUT
        msg = Message()
        msg.from_id = server.id
        msg.to_id = peer
        msg.type = MessageType.RequestVote
        msg.direction = Direction.Request
        msg.payload = {
            'term': server.term,
            'lastLogTerm': log_term(server.log, len(server.log)),
            'lastLogIndex': len(server.log)
        }
        send_message(model, msg)

def rules_send_append_entries(model, server, peer):
    if server.state == ServerState.Leader:
        if (server.heartbeat_due[peer] <= model.time or 
            (server.next_index[peer] <= len(server.log) and server.rpc_due[peer] <= model.time)):
            
            prev_index = server.next_index[peer] - 1
            last_index = min(prev_index + BATCH_SIZE, len(server.log))
            if server.match_index[peer] + 1 < server.next_index[peer]:
                last_index = prev_index
            
            msg = Message()
            msg.from_id = server.id
            msg.to_id = peer
            msg.type = MessageType.AppendEntries
            msg.direction = Direction.Request
            
            entries = server.log[prev_index:last_index]
            msg.payload = {
                'term': server.term,
                'prevIndex': prev_index,
                'prevTerm': log_term(server.log, prev_index),
                'entries': entries,
                'commitIndex': min(server.commit_index, last_index)
            }
            send_message(model, msg)
            server.rpc_due[peer] = model.time + RPC_TIMEOUT
            server.heartbeat_due[peer] = model.time + ELECTION_TIMEOUT / 2

def rules_advance_commit_index(model, server):
    if server.state == ServerState.Leader:
        indexes = list(server.match_index.values()) + [len(server.log)]
        indexes.sort()
        n = indexes[NUM_SERVERS // 2]
        if log_term(server.log, n) == server.term:
            server.commit_index = max(server.commit_index, n)

# --- Handlers ---

def handle_request_vote(model, server, msg):
    if msg.direction == Direction.Request:
        req = msg.payload
        if server.term < req['term']: step_down(model, server, req['term'])
        granted = False
        if (server.term == req['term'] and 
            (server.voted_for is None or server.voted_for == msg.from_id) and 
            (req['lastLogTerm'] > log_term(server.log, len(server.log)) or 
             (req['lastLogTerm'] == log_term(server.log, len(server.log)) and req['lastLogIndex'] >= len(server.log)))):
            granted = True
            server.voted_for = msg.from_id
            server.election_alarm = make_election_alarm(model.time)
        
        reply = Message()
        reply.from_id = server.id
        reply.to_id = msg.from_id
        reply.type = MessageType.RequestVote
        reply.direction = Direction.Reply
        reply.payload = {'term': server.term, 'granted': granted}
        send_message(model, reply)
    else: # Reply
        reply = msg.payload
        if server.term < reply['term']: step_down(model, server, reply['term'])
        if server.state == ServerState.Candidate and server.term == reply['term']:
            server.rpc_due[msg.from_id] = float('inf')
            server.vote_granted[msg.from_id] = reply['granted']

def handle_append_entries(model, server, msg):
    if msg.direction == Direction.Request:
        req = msg.payload
        if server.term < req['term']: step_down(model, server, req['term'])
        success = False
        match_index = 0
        if server.term == req['term']:
            server.state = ServerState.Follower
            server.election_alarm = make_election_alarm(model.time)
            if (req['prevIndex'] == 0 or 
                (req['prevIndex'] <= len(server.log) and log_term(server.log, req['prevIndex']) == req['prevTerm'])):
                success = True
                index = req['prevIndex']
                for entry in req['entries']:
                    index += 1
                    if log_term(server.log, index) != entry.term:
                        server.log = server.log[:index-1]
                        server.log.append(entry)
                match_index = index
                server.commit_index = max(server.commit_index, req['commitIndex'])
        
        reply = Message()
        reply.from_id = server.id
        reply.to_id = msg.from_id
        reply.type = MessageType.AppendEntries
        reply.direction = Direction.Reply
        reply.payload = {'term': server.term, 'success': success, 'matchIndex': match_index}
        send_message(model, reply)
    else: # Reply
        reply = msg.payload
        if server.term < reply['term']: step_down(model, server, reply['term'])
        if server.state == ServerState.Leader and server.term == reply['term']:
            if reply['success']:
                server.match_index[msg.from_id] = max(server.match_index[msg.from_id], reply['matchIndex'])
                server.next_index[msg.from_id] = reply['matchIndex'] + 1
            else:
                server.next_index[msg.from_id] = max(1, server.next_index[msg.from_id] - 1)
            server.rpc_due[msg.from_id] = 0

def raft_update(model):
    # Rules
    for s in model.servers:
        if s.state == ServerState.Stopped: continue
        rules_start_new_election(model, s)
        rules_become_leader(model, s)
        rules_advance_commit_index(model, s)
        for p in s.peers:
            rules_send_request_vote(model, s, p)
            rules_send_append_entries(model, s, p)
    
    # Message Delivery
    keep = []
    deliver = []
    for m in model.messages:
        if m.recv_time <= model.time: deliver.append(m)
        elif m.recv_time < float('inf'): keep.append(m)
    model.messages = keep
    
    for m in deliver:
        for s in model.servers:
            if s.id == m.to_id: 
                if s.state != ServerState.Stopped:
                    if m.type == MessageType.RequestVote: handle_request_vote(model, s, m)
                    elif m.type == MessageType.AppendEntries: handle_append_entries(model, s, m)

# --- User Actions (The functions you asked for) ---

def action_stop(model, server):
    server.state = ServerState.Stopped
    server.election_alarm = 0

def action_resume(model, server):
    server.state = ServerState.Follower
    server.election_alarm = make_election_alarm(model.time)

def action_restart(model, server):
    action_stop(model, server)
    action_resume(model, server)

def action_timeout(model, server):
    # Force election immediately
    server.state = ServerState.Follower
    server.election_alarm = 0
    rules_start_new_election(model, server)

def action_client_request(model, server):
    if server.state == ServerState.Leader:
        server.log.append(LogEntry(server.term, 'v'))

# ==========================================
# 4. 界面实现 (PyQt5)
# ==========================================

class ServerItem(QGraphicsItem):
    def __init__(self, server_id, total_servers, action_callback):
        super().__init__()
        self.server_id = server_id
        self.action_callback = action_callback # Function to handle menu actions
        self.ring_cx, self.ring_cy, self.ring_r = 300, 250, 150
        self.radius = 30
        
        c = circle_coord((server_id - 1) / total_servers, self.ring_cx, self.ring_cy, self.ring_r)
        self.cx, self.cy = c['x'], c['y']
        self.rect = QRectF(self.cx - 40, self.cy - 40, 80, 80) # Hitbox slightly larger
        
        # Render cache
        self.term = 1
        self.state = ServerState.Follower
        self.alarm_frac = 0

    def boundingRect(self):
        return self.rect

    def paint(self, painter, option, widget):
        # 1. Alarm Arc
        if self.state != ServerState.Stopped:
            r = self.radius + 4
            angle = -self.alarm_frac * 360 * 16
            painter.setPen(QPen(Qt.black, 4))
            painter.drawArc(int(self.cx-r), int(self.cy-r), int(r*2), int(r*2), 90*16, int(angle))

        # 2. Body
        color = QColor(TERM_COLORS[(self.term % len(TERM_COLORS))])
        if self.state == ServerState.Stopped: color = QColor("gray")
        painter.setBrush(QBrush(color))
        
        pen = QPen(Qt.black, 2)
        if self.state == ServerState.Leader:
            pen = QPen(QColor("#FFD700"), 5) # Gold thick border for Leader
        elif self.state == ServerState.Candidate:
            pen = QPen(Qt.blue, 3, Qt.DashLine)
            
        painter.setPen(pen)
        painter.drawEllipse(QPointF(self.cx, self.cy), self.radius, self.radius)

        # 3. Text
        painter.setPen(Qt.black)
        painter.setFont(QFont("Arial", 12, QFont.Bold))
        painter.drawText(QRectF(self.cx-20, self.cy-15, 40, 30), Qt.AlignCenter, f"S{self.server_id}")
        
        # 4. Leader Label
        if self.state == ServerState.Leader:
            painter.setFont(QFont("Arial", 10, QFont.Bold))
            painter.setPen(QColor("black"))
            painter.drawText(QRectF(self.cx-30, self.cy-50, 60, 20), Qt.AlignCenter, "(LEADER)")

        # 5. Term
        painter.setFont(QFont("Arial", 9))
        painter.setPen(Qt.black)
        painter.drawText(QRectF(self.cx-20, self.cy+self.radius+5, 40, 15), Qt.AlignCenter, f"Term {self.term}")

    def contextMenuEvent(self, event):
        menu = QMenu()
        
        act_req = menu.addAction("Request (Client)")
        act_stop = menu.addAction("Stop")
        act_resume = menu.addAction("Resume")
        act_restart = menu.addAction("Restart")
        act_timeout = menu.addAction("Time out")
        
        # Enable/Disable logic based on state
        if self.state == ServerState.Stopped:
            act_req.setEnabled(False)
            act_stop.setEnabled(False)
            act_timeout.setEnabled(False)
        else:
            act_resume.setEnabled(False)
            if self.state != ServerState.Leader:
                act_req.setEnabled(False)

        action = menu.exec_(event.screenPos())
        
        if action == act_stop: self.action_callback(self.server_id, "stop")
        elif action == act_resume: self.action_callback(self.server_id, "resume")
        elif action == act_restart: self.action_callback(self.server_id, "restart")
        elif action == act_timeout: self.action_callback(self.server_id, "timeout")
        elif action == act_req: self.action_callback(self.server_id, "request")

class MessageItem(QGraphicsItem):
    def __init__(self, msg, p1, p2, progress, success):
        super().__init__()
        self.msg = msg
        self.p1, self.p2 = p1, p2
        self.progress = progress
        self.success = success
    
    def boundingRect(self): return QRectF(0, 0, 600, 500)
    
    def paint(self, painter, option, widget):
        cx = self.p1['x'] + (self.p2['x'] - self.p1['x']) * self.progress
        cy = self.p1['y'] + (self.p2['y'] - self.p1['y']) * self.progress
        
        painter.setPen(Qt.NoPen)
        if self.msg.type == MessageType.RequestVote: painter.setBrush(QColor("#fbb4ae"))
        else: painter.setBrush(QColor("#b3cde3"))
        
        painter.drawEllipse(QPointF(cx, cy), 8, 8)
        
        # Simple Arrow/Tick
        if self.msg.direction == Direction.Reply and self.success:
            painter.setPen(QPen(Qt.green, 2))
            painter.drawLine(int(cx-4), int(cy), int(cx), int(cy+4))
            painter.drawLine(int(cx), int(cy+4), int(cx+5), int(cy-5))

class LogWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.model = None
        self.setMinimumHeight(200)

    def paintEvent(self, event):
        if not self.model: return
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        
        margin, row_h, box_w, box_h = 50, 30, 25, 20
        
        # Headers
        p.drawText(5, 20, "Index")
        for i in range(1, 15):
            p.drawText(QRectF(margin + (i-1)*30, 5, 25, 20), Qt.AlignCenter, str(i))

        for i, s in enumerate(self.model.servers):
            y = 35 + i * row_h
            p.setPen(Qt.black)
            
            # Label: S1 (Leader)
            label = f"S{s.id}"
            if s.state == ServerState.Leader: label += "*"
            p.drawText(5, y+15, label)
            
            for idx, entry in enumerate(s.log):
                x = margin + idx * 30
                color = QColor(TERM_COLORS[entry.term % len(TERM_COLORS)])
                
                # Commit border
                if idx + 1 <= s.commit_index: p.setPen(QPen(Qt.black, 2))
                else: p.setPen(QPen(Qt.black, 1, Qt.DashLine))
                
                p.setBrush(color)
                p.drawRect(int(x), int(y), int(box_w), int(box_h))
                p.setPen(Qt.black)
                p.drawText(QRectF(x, y, box_w, box_h), Qt.AlignCenter, str(entry.term))

class RaftWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("RaftScope Desktop")
        self.resize(1000, 800)
        
        self.model = RaftModel()
        self.paused = False
        self.speed = 0.2
        
        # UI Setup
        w = QWidget()
        self.setCentralWidget(w)
        layout = QVBoxLayout(w)
        
        # Top Scene
        self.scene = QGraphicsScene(0, 0, 600, 500)
        self.view = QGraphicsView(self.scene)
        self.view.setRenderHint(QPainter.Antialiasing)
        layout.addWidget(self.view, 2)
        
        # Controls
        ctrl = QHBoxLayout()
        self.lbl_info = QLabel("Time: 0.0s")
        ctrl.addWidget(self.lbl_info)
        
        btn_pause = QPushButton("Pause/Resume")
        btn_pause.clicked.connect(lambda: setattr(self, 'paused', not self.paused))
        ctrl.addWidget(btn_pause)
        
        slider = QSlider(Qt.Horizontal)
        slider.setRange(0, 100)
        slider.setValue(10)
        self.speed = 0.1
        
        slider.valueChanged.connect(lambda v: setattr(self, 'speed', v / 100.0))
        ctrl.addWidget(QLabel("Speed:"))
        ctrl.addWidget(slider)
        ctrl.addStretch()
        layout.addLayout(ctrl)
        
        # Log
        self.log_widget = LogWidget()
        layout.addWidget(self.log_widget, 1)
        
        # Init Items
        self.server_items = {}
        self.scene.addEllipse(150, 100, 300, 300, QPen(QColor("#eee"), 2))
        for s in self.model.servers:
            item = ServerItem(s.id, NUM_SERVERS, self.handle_node_action)
            self.scene.addItem(item)
            self.server_items[s.id] = item
            
        self.msg_items = []
        
        # Timer
        self.timer = QTimer()
        self.timer.timeout.connect(self.step)
        self.timer.start(16)

    def handle_node_action(self, server_id, action):
        # Find the server object in the model
        server = next(s for s in self.model.servers if s.id == server_id)
        
        if action == "stop": action_stop(self.model, server)
        elif action == "resume": action_resume(self.model, server)
        elif action == "restart": action_restart(self.model, server)
        elif action == "timeout": action_timeout(self.model, server)
        elif action == "request": action_client_request(self.model, server)
        
        self.update_ui()

    def step(self):
        if not self.paused:
            dt = 16000 * self.speed
            target = self.model.time + dt
            while self.model.time < target:
                chunk = min(1000, target - self.model.time)
                self.model.time += chunk
                raft_update(self.model)
            self.update_ui()

    def update_ui(self):
        self.lbl_info.setText(f"Time: {self.model.time/1e6:.2f}s")
        
        # Sync Server Items
        for s in self.model.servers:
            item = self.server_items[s.id]
            item.term = s.term
            item.state = s.state
            if s.election_alarm != float('inf') and s.state != ServerState.Stopped:
                remain = max(0, s.election_alarm - self.model.time)
                item.alarm_frac = remain / (ELECTION_TIMEOUT * 2)
            else:
                item.alarm_frac = 0
            item.update()
            
        # Sync Messages
        for m in self.msg_items: self.scene.removeItem(m)
        self.msg_items.clear()
        
        for m in self.model.messages:
            elapsed = self.model.time - m.send_time
            total = m.recv_time - m.send_time
            if total > 0 and elapsed >= 0 and elapsed <= total:
                s_node = self.server_items[m.from_id]
                e_node = self.server_items[m.to_id]
                p1 = {'x': s_node.cx, 'y': s_node.cy}
                p2 = {'x': e_node.cx, 'y': e_node.cy}
                
                success = False
                if m.direction == Direction.Reply:
                    success = m.payload.get('granted') or m.payload.get('success')
                
                item = MessageItem(m, p1, p2, elapsed/total, success)
                self.scene.addItem(item)
                self.msg_items.append(item)
                
        self.log_widget.model = self.model
        self.log_widget.update()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = RaftWindow()
    w.show()
    sys.exit(app.exec_())