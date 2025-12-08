import sys
import asyncio
import qasync
from PyQt5.QtWidgets import (QApplication, QMainWindow, QGraphicsScene, QGraphicsView, 
                             QGraphicsItem, QVBoxLayout, QWidget, QHBoxLayout, 
                             QLabel, QMenu)
from PyQt5.QtCore import Qt, QTimer, QRectF, QPointF
from PyQt5.QtGui import QPainter, QPen, QBrush, QColor, QFont


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from server import Node, Role
from config import clusters
from state import LogEntry

TERM_COLORS = ['#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3', '#a6d854', '#ffd92f']

class ServerItem(QGraphicsItem):
    def __init__(self, node: Node, total_nodes, index):
        super().__init__()
        self.node = node
        self.server_id = node.server.id
        
        # 布局计算
        self.ring_cx, self.ring_cy, self.ring_r = 300, 250, 150
        self.radius = 35
        import math
        radians = 2 * math.pi * (0.75 + index / total_nodes)
        self.cx = self.ring_cx + self.ring_r * math.cos(radians)
        self.cy = self.ring_cy + self.ring_r * math.sin(radians)
        
        self.rect = QRectF(self.cx - 40, self.cy - 40, 80, 80)

    def boundingRect(self):
        return self.rect

    def paint(self, painter, option, widget):
        server = self.node.server
        state = server.state
        
        # 1. Body Color (Based on Term)
        if self.node.stopped:
            color = QColor("gray")
        else:
            color = QColor(TERM_COLORS[(state.getCurrentTerm() % len(TERM_COLORS))])
        
        painter.setBrush(QBrush(color))
        
        # 2. Border (Based on Role)
        pen = QPen(Qt.black, 2)
        if not self.node.stopped:
            if self.node.server.role == Role.Leader:
                pen = QPen(QColor("#FFD700"), 6) # Gold
            elif self.node.server.role == Role.Candidate:
                pen = QPen(Qt.blue, 3, Qt.DashLine)
        
        painter.setPen(pen)
        painter.drawEllipse(QPointF(self.cx, self.cy), self.radius, self.radius)

        # 3. ID Label
        painter.setPen(Qt.black)
        painter.setFont(QFont("Arial", 12, QFont.Bold))
        painter.drawText(QRectF(self.cx-20, self.cy-15, 40, 30), Qt.AlignCenter, f"S{self.server_id}")
        
        # 4. Role Label
        if not self.node.stopped and self.node.server.role == Role.Leader:
            painter.setFont(QFont("Arial", 10, QFont.Bold))
            painter.drawText(QRectF(self.cx-30, self.cy-50, 60, 20), Qt.AlignCenter, "LEADER")

        # 5. Term Label
        painter.setFont(QFont("Arial", 9))
        txt = f"Term {state.getCurrentTerm()}"
        if self.node.stopped: txt = "STOPPED"
        painter.drawText(QRectF(self.cx-30, self.cy+self.radius+5, 60, 15), Qt.AlignCenter, txt)

    def contextMenuEvent(self, event):
        menu = QMenu()
        
        act_req = menu.addAction("Client Request")
        act_stop = menu.addAction("Stop Node")
        act_resume = menu.addAction("Resume Node")
        act_timeout = menu.addAction("Force Timeout")
        
        # 动态禁用逻辑
        if self.node.stopped:
            act_req.setEnabled(False)
            act_stop.setEnabled(False)
            act_timeout.setEnabled(False)
        else:
            act_resume.setEnabled(False)
            if self.node.server.role != Role.Leader:
                act_req.setEnabled(False)

        action = menu.exec_(event.screenPos())
        
        if action == act_stop: self.node.stop()
        elif action == act_resume: self.node.resume()
        elif action == act_timeout: self.node.force_timeout()
        elif action == act_req: self.node.client_request()
        
        self.scene().update()

class LogWidget(QWidget):
    def __init__(self, nodes):
        super().__init__()
        self.nodes = nodes
        self.setMinimumHeight(250)

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.Antialiasing)
        
        margin_left, row_h, box_w, box_h = 60, 35, 30, 25
        
        # Draw Axis
        p.drawText(5, 20, "Log Idx")
        for i in range(1, 20):
            p.drawText(QRectF(margin_left + (i)*box_w, 5, box_w, 20), Qt.AlignCenter, str(i))

        for i, node in enumerate(self.nodes):
            y = 35 + i * row_h
            p.setPen(Qt.black)
            
            # Row Header
            label = f"S{node.server.id}"
            if node.server.role == Role.Leader and not node.stopped: label += "*"
            if node.stopped: p.setPen(Qt.gray)
            p.drawText(5, y+18, label)
            
            # Draw Logs
            logs = node.server.state.getLog()
            commit_idx = node.server.state.getCommitId()
            
            for idx, entry in enumerate(logs):
                if idx == 0: continue # Skip dummy entry at index 0
                
                x = margin_left + idx * box_w
                
                # Color by Term
                color = QColor(TERM_COLORS[entry.term % len(TERM_COLORS)])
                if node.stopped: color = QColor("#eee")
                
                # Border (Solid for committed, Dashed for uncommitted)
                if idx <= commit_idx: 
                    p.setPen(QPen(Qt.black, 2))
                else: 
                    p.setPen(QPen(Qt.black, 1, Qt.DashLine))
                
                p.setBrush(color)
                p.drawRect(int(x), int(y), int(box_w), int(box_h))
                
                p.setPen(Qt.black)
                val_str = str(entry.cmd) if isinstance(entry.cmd, int) else str(entry.cmd)[:3]
                p.drawText(QRectF(x, y, box_w, box_h), Qt.AlignCenter, str(entry.term))

class RaftVisualizer(QMainWindow):
    def __init__(self, nodes):
        super().__init__()
        self.nodes = nodes
        self.setWindowTitle("Real Raft Implementation Visualizer")
        self.resize(1000, 800)
        
        w = QWidget()
        self.setCentralWidget(w)
        layout = QVBoxLayout(w)
        
        # 1. Top: Network Topology
        self.scene = QGraphicsScene(0, 0, 600, 500)
        self.view = QGraphicsView(self.scene)
        self.view.setRenderHint(QPainter.Antialiasing)
        layout.addWidget(self.view, 3)
        
        # 2. Bottom: Log Table
        self.log_widget = LogWidget(nodes)
        layout.addWidget(self.log_widget, 2)
        
        # Initialize Graphics Items
        self.scene.addEllipse(150, 100, 300, 300, QPen(QColor("#eee"), 2))
        
        self.node_items = []
        for i, node in enumerate(nodes):
            item = ServerItem(node, len(nodes), i)
            self.scene.addItem(item)
            self.node_items.append(item)
            
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_ui)
        self.timer.start(100) 

    def refresh_ui(self):
        self.scene.update() # Refresh graphics items (colors, roles)
        self.log_widget.update() # Refresh log table

def main():
    app = QApplication(sys.argv)
    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop) 

    nodes = []
    print("Initializing Nodes...")
    for port in clusters.keys():
        node = Node(port)
        nodes.append(node)
        
    for node in nodes:
        loop.create_task(node.start())

    win = RaftVisualizer(nodes)
    win.show()

    with loop:
        loop.run_forever()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass