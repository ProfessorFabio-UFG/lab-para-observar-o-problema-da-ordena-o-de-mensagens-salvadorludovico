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
message_queue = []  # Priority queue: (timestamp, sender_id, msg)
queue_lock = threading.Lock()

# Set to track messages already in queue (avoid duplicates)
messages_in_queue = set()

# Track acknowledgments received for each message
acks_received = defaultdict(set)  # {(timestamp, sender_id, msg_num): set of ack_senders}
acks_lock = threading.Lock()

# Track messages we've already seen to avoid duplicates
seen_messages = set()
seen_lock = threading.Lock()

# Delivered messages log
delivered_messages = []
delivery_counter = 0
delivery_lock = threading.Lock()

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = []
myself = -1
nMsgs = 0

# UDP sockets to send and receive data messages:
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def update_lamport_clock(received_timestamp=None):
    """Update Lamport clock according to the rules"""
    global lamport_clock
    with clock_lock:
        if received_timestamp is not None:
            lamport_clock = max(lamport_clock, received_timestamp) + 1
        else:
            lamport_clock += 1
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
    print('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
    req = {"op":"list"}
    msg = pickle.dumps(req)
    print('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    print('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS

def broadcast_message(msg_type, content):
    """Broadcast a message to all peers"""
    for addrToSend in PEERS:
        sendSocket.sendto(content, (addrToSend, PEER_UDP_PORT))

def can_deliver_message(timestamp, sender_id, msg_num):
    """Check if a message can be delivered according to Lamport's algorithm"""
    # A message can be delivered when we have received acknowledgments from all other processes
    with acks_lock:
        ack_count = len(acks_received.get((timestamp, sender_id, msg_num), set()))
        # We need acks from all N processes
        return ack_count >= N

def try_deliver_messages():
    """Try to deliver messages that are ready"""
    global delivery_counter
    
    with queue_lock:
        delivered_any = True
        while delivered_any and message_queue:
            delivered_any = False
            # Check if the top message can be delivered
            if message_queue:
                timestamp, sender_id, msg_num = message_queue[0]
                
                if can_deliver_message(timestamp, sender_id, msg_num):
                    # Remove from queue
                    heapq.heappop(message_queue)
                    messages_in_queue.discard((timestamp, sender_id, msg_num))
                    
                    # Deliver the message
                    with delivery_lock:
                        delivery_counter += 1
                        log_entry = f"[P{myself}] | received_from: P{sender_id} | msg_id: m{msg_num} | timestamp: {timestamp} | delivery_order: {delivery_counter}"
                        print(log_entry)
                        delivered_messages.append((sender_id, msg_num))
                    
                    delivered_any = True

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock
        self.sock.settimeout(1.0)  # Add timeout to avoid blocking forever

    def run(self):
        global handShakeCount, lamport_clock, message_queue, acks_received, delivered_messages, delivery_counter
        
        print('Handler is ready. Waiting for the handshakes...')
        
        # Wait until handshakes are received from all other processes
        while handShakeCount < N:
            try:
                msgPack = self.sock.recv(1024)
                msg = pickle.loads(msgPack)
                if msg[0] == 'READY':
                    handShakeCount = handShakeCount + 1
                    print('--- Handshake received: ', msg[1])
            except timeout:
                continue

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        expected_stop_count = N  # We expect STOP from all N peers including ourselves
        
        while stopCount < expected_stop_count:
            try:
                msgPack = self.sock.recv(1024)
                msg = pickle.loads(msgPack)
                
                if msg[0] == 'STOP':
                    # Stop signal
                    stopCount = stopCount + 1
                    print(f'Received STOP signal {stopCount}/{expected_stop_count}')
                
                elif msg[0] == 'DATA':
                    # Data message: ('DATA', sender_id, msg_number, lamport_timestamp)
                    _, sender_id, msg_number, sender_timestamp = msg
                    
                    # Check if we've seen this message before
                    message_id = (sender_id, msg_number, sender_timestamp)
                    with seen_lock:
                        if message_id in seen_messages:
                            continue  # Skip duplicate
                        seen_messages.add(message_id)
                    
                    # Update Lamport clock
                    update_lamport_clock(sender_timestamp)
                    
                    # Add to message queue if not already there
                    with queue_lock:
                        queue_entry = (sender_timestamp, sender_id, msg_number)
                        if queue_entry not in messages_in_queue:
                            heapq.heappush(message_queue, queue_entry)
                            messages_in_queue.add(queue_entry)
                    
                    # Send acknowledgment to all peers
                    ack_timestamp = update_lamport_clock()
                    ack_msg = ('ACK', myself, sender_id, msg_number, sender_timestamp, ack_timestamp)
                    ack_pack = pickle.dumps(ack_msg)
                    broadcast_message('ACK', ack_pack)
                    
                    # Record our own ack
                    with acks_lock:
                        acks_received[(sender_timestamp, sender_id, msg_number)].add(myself)
                    
                    # Try to deliver messages
                    try_deliver_messages()
                
                elif msg[0] == 'ACK':
                    # Acknowledgment: ('ACK', ack_sender, original_sender, msg_number, original_timestamp, ack_timestamp)
                    _, ack_sender, original_sender, msg_number, original_timestamp, ack_timestamp = msg
                    
                    # Update Lamport clock
                    update_lamport_clock(ack_timestamp)
                    
                    # Record the acknowledgment
                    with acks_lock:
                        acks_received[(original_timestamp, original_sender, msg_number)].add(ack_sender)
                    
                    # Try to deliver messages
                    try_deliver_messages()
                    
            except timeout:
                # Check if we can deliver any pending messages
                try_deliver_messages()
                continue
            except Exception as e:
                print(f"Error in message handler: {e}")
                continue
        
        # Wait a bit more to ensure all messages are delivered
        print("Waiting for final message delivery...")
        time.sleep(2)
        try_deliver_messages()
        
        # Write log file
        logFile = open('logfile'+str(myself)+'.log', 'w')
        for entry in delivered_messages:
            logFile.write(f"{entry}\n")
        logFile.close()
        
        # Send the list of messages to the server for comparison
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(delivered_messages)
        clientSock.send(msgPack)
        clientSock.close()
        
        # Reset for next round
        lamport_clock = 0
        message_queue = []
        messages_in_queue.clear()
        acks_received.clear()
        seen_messages.clear()
        delivered_messages = []
        delivery_counter = 0
        handShakeCount = 0

def waitToStart():
    """Wait for start signal from comparison server"""
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
    conn.close()
    return (myself, nMsgs)

# Main execution
registerWithGroupManager()

while True:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    # Wait for other processes to be ready
    time.sleep(5)

    # Create receiving message handler
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()
    
    # Send handshakes
    for addrToSend in PEERS:
        print('Sending handshake to ', addrToSend)
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while handShakeCount < N:
        time.sleep(0.1)

    # Send a sequence of data messages to all other processes
    for msgNumber in range(0, nMsgs):
        # Wait some random time between successive messages
        time.sleep(random.randrange(10, 100) / 1000)
        
        # Update Lamport clock and send message
        timestamp = update_lamport_clock()
        msg = ('DATA', myself, msgNumber, timestamp)
        msgPack = pickle.dumps(msg)
        
        # Broadcast to all peers
        broadcast_message('DATA', msgPack)
        
        # Add our own message to the queue and record our ack
        with queue_lock:
            queue_entry = (timestamp, myself, msgNumber)
            if queue_entry not in messages_in_queue:
                heapq.heappush(message_queue, queue_entry)
                messages_in_queue.add(queue_entry)
        
        with acks_lock:
            acks_received[(timestamp, myself, msgNumber)].add(myself)
        
        # Add to seen messages
        with seen_lock:
            seen_messages.add((myself, msgNumber, timestamp))
        
        # Try to deliver messages
        try_deliver_messages()
        
        print(f'Sent message {msgNumber} with timestamp {timestamp}')

    # Give some time for all messages to be processed
    time.sleep(1)

    # Tell all processes that I have no more messages to send
    for addrToSend in PEERS:
        msg = ('STOP', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    
    # Wait for handler to finish
    msgHandler.join()