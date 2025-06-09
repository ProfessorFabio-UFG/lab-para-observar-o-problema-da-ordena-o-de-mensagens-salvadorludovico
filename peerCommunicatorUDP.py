from socket  import *
from constMP import * 
import threading
import random
import time
import pickle
from requests import get
import heapq
from collections import defaultdict

# Lamport clock for this process
lamport_clock = 0
clock_lock = threading.Lock()

# Message queue ordered by Lamport timestamp
message_queue = []  # Priority queue: (timestamp, sender_id, msg_num)
queue_lock = threading.Lock()

# Track acknowledgments received for each message
# Key: (original_timestamp, original_sender, msg_num), Value: set of (ack_sender, ack_timestamp)
acks_received = defaultdict(set)
acks_lock = threading.Lock()

# Track handshake confirmations
handshake_confirmations = set()
handshake_lock = threading.Lock()

# Delivered messages log
delivered_messages = []
delivery_counter = 0
delivery_lock = threading.Lock()

# Counter for handshakes
handShakeCount = 0

PEERS = []
myself = -1
nMsgs = 0

# UDP sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket for comparison server
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def update_lamport_clock(received_timestamp=None):
    """Update Lamport clock according to Lamport's rules"""
    global lamport_clock
    with clock_lock:
        if received_timestamp is not None:
            lamport_clock = max(lamport_clock, received_timestamp) + 1
        else:
            lamport_clock += 1
        return lamport_clock

def get_current_clock():
    """Get current clock value without updating"""
    with clock_lock:
        return lamport_clock

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    clientSock.close()
    return PEERS

def send_with_retries(msg, addr, max_retries=3):
    """Send message with retries"""
    for i in range(max_retries):
        sendSocket.sendto(msg, (addr, PEER_UDP_PORT))
        time.sleep(0.01 * (i + 1))  # Exponential backoff

def broadcast_message(content):
    """Broadcast a message to all peers"""
    for addrToSend in PEERS:
        sendSocket.sendto(content, (addrToSend, PEER_UDP_PORT))

def can_deliver_message(timestamp, sender_id, msg_num):
    """
    Check if a message can be delivered according to Lamport's total ordering algorithm.
    A message m can be delivered when:
    1. m is at the head of the queue
    2. For all processes p, we have received a message with timestamp > m.timestamp
    """
    with acks_lock:
        ack_senders = acks_received.get((timestamp, sender_id, msg_num), set())
        
        # We need to have received an ACK from every process
        if len(ack_senders) < N:
            return False
        
        # Check that all ACKs have timestamps greater than the message timestamp
        # This ensures all processes have seen this message
        for ack_sender, ack_timestamp in ack_senders:
            if ack_timestamp <= timestamp:
                return False
                
        return True

def try_deliver_messages():
    """Try to deliver messages that are ready according to total ordering"""
    global delivery_counter
    
    with queue_lock:
        delivered = True
        while delivered and message_queue:
            delivered = False
            
            # Peek at the head of the queue
            if message_queue:
                timestamp, sender_id, msg_num = message_queue[0]
                
                # Check if this message can be delivered
                if can_deliver_message(timestamp, sender_id, msg_num):
                    # Remove from queue
                    heapq.heappop(message_queue)
                    
                    # Deliver the message
                    with delivery_lock:
                        delivery_counter += 1
                        log_entry = f"[P{myself}] | received_from: P{sender_id} | msg_id: m{msg_num} | timestamp: {timestamp} | delivery_order: {delivery_counter}"
                        print(log_entry)
                        delivered_messages.append((sender_id, msg_num))
                    
                    delivered = True

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.running = True

    def run(self):
        global handShakeCount
        
        print('Handler is ready. Waiting for handshakes...')
        
        # Phase 1: Handshake exchange with confirmation
        handshake_timeout = time.time() + 30  # 30 second timeout
        
        while handShakeCount < N and time.time() < handshake_timeout:
            try:
                self.sock.settimeout(0.5)
                msgPack, addr = self.sock.recvfrom(1024)
                msg = pickle.loads(msgPack)
                
                if msg[0] == 'HANDSHAKE':
                    sender = msg[1]
                    print(f'--- Handshake received from P{sender}')
                    
                    # Send confirmation
                    confirm_msg = ('HANDSHAKE_CONFIRM', myself, sender)
                    confirm_pack = pickle.dumps(confirm_msg)
                    sendSocket.sendto(confirm_pack, addr)
                    
                    with handshake_lock:
                        if sender not in handshake_confirmations:
                            handshake_confirmations.add(sender)
                            handShakeCount = len(handshake_confirmations)
                
                elif msg[0] == 'HANDSHAKE_CONFIRM':
                    confirmer = msg[1]
                    original_sender = msg[2]
                    
                    if original_sender == myself:
                        with handshake_lock:
                            if confirmer not in handshake_confirmations:
                                handshake_confirmations.add(confirmer)
                                handShakeCount = len(handshake_confirmations)
                                print(f'--- Handshake confirmed by P{confirmer}')
                                
            except timeout:
                continue
            except Exception as e:
                print(f"Handshake error: {e}")
                continue

        if handShakeCount < N:
            print(f"WARNING: Only received {handShakeCount}/{N} handshake confirmations")
        else:
            print(f"All {N} handshakes confirmed. Starting message exchange...")

        # Phase 2: Message exchange
        self.sock.settimeout(1.0)
        stop_count = 0
        message_count = 0
        
        while stop_count < N and self.running:
            try:
                msgPack, addr = self.sock.recvfrom(4096)
                msg = pickle.loads(msgPack)
                
                if msg[0] == 'STOP':
                    stop_count += 1
                    print(f'Received STOP {stop_count}/{N}')
                
                elif msg[0] == 'DATA':
                    # Data message: ('DATA', sender_id, msg_number, lamport_timestamp)
                    _, sender_id, msg_number, sender_timestamp = msg
                    
                    # Update Lamport clock
                    current_time = update_lamport_clock(sender_timestamp)
                    
                    # Add to message queue
                    with queue_lock:
                        # Use tuple for proper ordering: (timestamp, sender_id, msg_num)
                        heapq.heappush(message_queue, (sender_timestamp, sender_id, msg_number))
                    
                    # Send ACK to all processes
                    ack_msg = ('ACK', myself, sender_id, msg_number, sender_timestamp, current_time)
                    ack_pack = pickle.dumps(ack_msg)
                    broadcast_message(ack_pack)
                    
                    # Record our own ACK
                    with acks_lock:
                        acks_received[(sender_timestamp, sender_id, msg_number)].add((myself, current_time))
                    
                    message_count += 1
                    
                    # Try to deliver messages
                    try_deliver_messages()
                
                elif msg[0] == 'ACK':
                    # ACK: ('ACK', ack_sender, original_sender, msg_number, original_timestamp, ack_timestamp)
                    _, ack_sender, original_sender, msg_number, original_timestamp, ack_timestamp = msg
                    
                    # Update Lamport clock
                    update_lamport_clock(ack_timestamp)
                    
                    # Record the ACK
                    with acks_lock:
                        acks_received[(original_timestamp, original_sender, msg_number)].add((ack_sender, ack_timestamp))
                    
                    # Try to deliver messages
                    try_deliver_messages()
                    
            except timeout:
                # Periodically try to deliver messages
                try_deliver_messages()
            except Exception as e:
                print(f"Message handling error: {e}")
                continue

        print(f"Received {message_count} messages total")
        
        # Final delivery attempt
        print("Final delivery check...")
        time.sleep(1)
        try_deliver_messages()
        
        # Log results
        print(f"Total delivered: {len(delivered_messages)}")
        
        # Write log file
        with open(f'logfile{myself}.log', 'w') as logFile:
            for entry in delivered_messages:
                logFile.write(f"{entry}\n")
        
        # Send results to comparison server
        print('Sending results to comparison server...')
        try:
            clientSock = socket(AF_INET, SOCK_STREAM)
            clientSock.connect((SERVER_ADDR, SERVER_PORT))
            msgPack = pickle.dumps(delivered_messages)
            clientSock.send(msgPack)
            clientSock.close()
        except Exception as e:
            print(f"Error sending to comparison server: {e}")

    def stop(self):
        self.running = False

def waitToStart():
    """Wait for start signal from comparison server"""
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps(f'Peer process {myself} started.'))
    conn.close()
    return (myself, nMsgs)

def reset_state():
    """Reset all state for next round"""
    global lamport_clock, message_queue, acks_received, delivered_messages
    global delivery_counter, handShakeCount, handshake_confirmations
    
    lamport_clock = 0
    message_queue = []
    acks_received.clear()
    delivered_messages = []
    delivery_counter = 0
    handShakeCount = 0
    handshake_confirmations.clear()

# Main execution
registerWithGroupManager()

while True:
    print('\n=== Waiting for signal to start ===')
    (myself, nMsgs) = waitToStart()
    print(f'Starting as Peer {myself} with {nMsgs} messages to send')
    
    if nMsgs == 0:
        print('Terminating.')
        break
    
    # Reset state from previous round
    reset_state()
    
    # Get peer list
    PEERS = getListOfPeers()
    print(f'Peer list: {PEERS}')
    
    # Start message handler
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    
    # Wait a bit for all peers to start
    time.sleep(3)
    
    # Send handshakes with retries
    print("Sending handshakes...")
    for i in range(5):  # Try 5 times
        for addr in PEERS:
            msg = ('HANDSHAKE', myself)
            msgPack = pickle.dumps(msg)
            sendSocket.sendto(msgPack, (addr, PEER_UDP_PORT))
        
        time.sleep(0.5)
        
        with handshake_lock:
            if len(handshake_confirmations) >= N:
                break
    
    # Wait for all handshakes
    timeout = time.time() + 10
    while handShakeCount < N and time.time() < timeout:
        time.sleep(0.1)
    
    print(f"Handshake phase complete: {handShakeCount}/{N}")
    
    # Send messages
    for msgNumber in range(nMsgs):
        # Random delay
        time.sleep(random.randrange(10, 100) / 1000)
        
        # Update clock and create message
        timestamp = update_lamport_clock()
        msg = ('DATA', myself, msgNumber, timestamp)
        msgPack = pickle.dumps(msg)
        
        # Add to our own queue
        with queue_lock:
            heapq.heappush(message_queue, (timestamp, myself, msgNumber))
        
        # Record our own ACK
        with acks_lock:
            acks_received[(timestamp, myself, msgNumber)].add((myself, timestamp + 1))
        
        # Broadcast message
        broadcast_message(msgPack)
        
        print(f'Sent message {msgNumber} with timestamp {timestamp}')
        
        # Try to deliver
        try_deliver_messages()
    
    # Wait a bit for message propagation
    time.sleep(2)
    
    # Send STOP signal
    print("Sending STOP signals...")
    stop_msg = ('STOP', myself)
    stop_pack = pickle.dumps(stop_msg)
    for _ in range(3):  # Send multiple times
        broadcast_message(stop_pack)
        time.sleep(0.1)
    
    # Wait for handler to finish
    msgHandler.join(timeout=30)
    
    if msgHandler.is_alive():
        print("Warning: Handler thread did not finish")
        msgHandler.stop()