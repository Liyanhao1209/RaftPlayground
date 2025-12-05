import zmq
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor

# --- 配置 ---
ENDPOINT = "tcp://127.0.0.1:5555"
REQUEST_TIMEOUT = 1000  # Client 等待响应的超时时间 (毫秒)
MAX_RETRIES = 3         # Client 最大重试次数

# 用于控制 Server 何时退出的信号（仅为了脚本能优雅结束）
stop_event = threading.Event()

def server_task():
    """
    模拟一个不稳定的服务器：会随机丢包（模拟崩溃）、延迟或正常响应。
    """
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(ENDPOINT)
    
    print(f"[Server] 启动监听 {ENDPOINT}")

    while not stop_event.is_set():
        try:
            # 使用非阻塞接收，以便能检查 stop_event 退出循环
            try:
                # 轮询 100ms 查看是否有请求
                if socket.poll(100) == 0:
                    continue
                message = socket.recv_string()
            except zmq.ZMQError:
                continue

            print(f"[Server] 收到请求: {message}")

            # --- 模拟故障场景 ---
            simulation = random.randint(1, 10)

            if simulation <= 3:
                # 【场景1】30% 概率：模拟服务器崩溃/断网 (丢弃请求，无响应)
                print(f"[Server] >>> 模拟崩溃/丢包 (忽略 {message})")
                # 模拟崩溃恢复：关闭当前 socket 并重新绑定
                # 这会导致 Client 端收不到回音，进而触发 Client 的超时机制
                socket.close()
                time.sleep(0.5) # 模拟重启时间
                socket = context.socket(zmq.REP)
                socket.bind(ENDPOINT)
                
            elif simulation <= 5:
                # 【场景2】20% 概率：模拟处理极慢 (超过 Client 的 1000ms 超时)
                print(f"[Server] >>> 模拟高延迟 (睡 2秒)")
                time.sleep(2.0)
                # 即使发回去，Client 此时可能已经判定超时并断开旧连接了
                # ZMQ 处理这种情况很智能，不会报错，只是消息丢失
                socket.send_string(f"Delayed Response {message}")
            
            else:
                # 【场景3】50% 概率：正常快速响应
                print(f"[Server] >>> 正常处理")
                time.sleep(0.2) # 模拟一点点处理时间
                socket.send_string(f"OK-{message}")

        except Exception as e:
            print(f"[Server] Error: {e}")
            break
    
    socket.close()
    context.term()
    print("[Server] 线程退出")

def client_task():
    """
    实现 Lazy Pirate 模式：超时 -> 关闭 Socket -> 重建 Socket -> 重传
    """
    context = zmq.Context()
    print("[Client] 启动")
    
    # 我们发送 5 个任务进行测试
    for sequence in range(1, 6):
        request_msg = str(sequence)
        
        # 1. 创建 Socket (初次连接)
        client = context.socket(zmq.REQ)
        client.connect(ENDPOINT)
        
        # 2. 初始化 Poller
        poller = zmq.Poller()
        poller.register(client, zmq.POLLIN)
        
        # 发送逻辑
        print(f"\n[Client] ------ 发送请求 {request_msg} ------")
        client.send_string(request_msg)
        
        retries_left = MAX_RETRIES
        expect_reply = True
        
        while expect_reply:
            # 3. 轮询等待响应
            socks = dict(poller.poll(REQUEST_TIMEOUT))
            
            if socks.get(client) == zmq.POLLIN:
                # A. 成功收到响应
                reply = client.recv_string()
                print(f"[Client] 收到响应: {reply}")
                
                # 这里的逻辑是：一次交互完成就销毁 socket (虽然这有点浪费，但在 REQ/REP 异常处理中是最安全的)
                # 或者你可以只注销 poller，但在 Lazy Pirate 模式中，
                # 每次“完成”或“失败”都建议清理状态，为下一次请求保持干净。
                poller.unregister(client)
                client.close()
                expect_reply = False 
                
            else:
                # B. 超时 (没有收到 POLLIN 事件)
                print(f"[Client] !!! 请求 {request_msg} 超时 !!!")
                
                # 必须操作：销毁旧 Socket
                poller.unregister(client)
                client.close()
                
                retries_left -= 1
                if retries_left == 0:
                    print(f"[Client] !!! 彻底失败：请求 {request_msg} 重试多次无果，放弃。")
                    expect_reply = False
                else:
                    print(f"[Client] >>> 重建连接并重试... (剩余次数: {retries_left})")
                    
                    # 重建 Socket
                    client = context.socket(zmq.REQ)
                    client.connect(ENDPOINT)
                    poller.register(client, zmq.POLLIN)
                    
                    # 再次发送
                    client.send_string(request_msg)

    print("\n[Client] 所有任务完成，通知 Server 退出...")
    stop_event.set() # 通知 Server 停止
    context.term()

if __name__ == "__main__":
    # 使用线程池并发运行 Server 和 Client
    with ThreadPoolExecutor(max_workers=2) as executor:
        # 先提交 Server，让它先跑起来
        executor.submit(server_task)
        # 稍微等一下确保 Server socket bind 成功 (虽然 ZMQ 支持先 connect 后 bind，但为了 log 顺序好看)
        time.sleep(0.5) 
        # 提交 Client
        executor.submit(client_task)
        
    print("主程序结束")