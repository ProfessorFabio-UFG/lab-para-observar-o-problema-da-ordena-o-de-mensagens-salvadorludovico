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

# Track acknowledgments received for each message
acks_received = defaultdict(set)  # {(timestamp, sender_id): set of ack_senders}
acks_lock = threading.Lock()

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

def can_deliver_message(timestamp, sender_id):
    """Check if a message can be delivered according to Lamport's algorithm"""
    # A message can be delivered when we have received acknowledgments from all other processes
    with acks_lock:
        ack_count = len(acks_received.get((timestamp, sender_id), set()))
        # We need acks from all N processes (including the original sender)
        return ack_count >= N

def try_deliver_messages():
    """Try to deliver messages that are ready"""
    global delivery_counter
    
    with queue_lock:
        while message_queue:
            # Peek at the top message
            timestamp, sender_id, msg_content = message_queue[0]
            
            if can_deliver_message(timestamp, sender_id):
                # Remove from queue
                heapq.heappop(message_queue)
                
                # Deliver the message
                with delivery_lock:
                    delivery_counter += 1
                    log_entry = f"[P{myself}] | received_from: P{sender_id} | msg_id: m{msg_content} | timestamp: {timestamp} | delivery_order: {delivery_counter}"
                    print(log_entry)
                    delivered_messages.append((sender_id, msg_content))
            else:
                # Can't deliver yet, stop checking
                break

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        
        global handShakeCount
        
        # Wait until handshakes are received from all other processes
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount = handShakeCount + 1
                print('--- Handshake received: ', msg[1])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            
            if msg[0] == 'STOP':
                # Stop signal
                stopCount = stopCount + 1
                if stopCount == N:
                    break
            
            elif msg[0] == 'DATA':
                # Data message: ('DATA', sender_id, msg_number, lamport_timestamp)
                _, sender_id, msg_number, sender_timestamp = msg
                
                # Update Lamport clock
                update_lamport_clock(sender_timestamp)
                
                # Add to message queue
                with queue_lock:
                    heapq.heappush(message_queue, (sender_timestamp, sender_id, msg_number))
                
                # Send acknowledgment to all peers
                ack_timestamp = update_lamport_clock()
                ack_msg = ('ACK', myself, sender_id, msg_number, sender_timestamp, ack_timestamp)
                ack_pack = pickle.dumps(ack_msg)
                broadcast_message('ACK', ack_pack)
                
                # Record our own ack
                with acks_lock:
                    acks_received[(sender_timestamp, sender_id)].add(myself)
                
                # Try to deliver messages
                try_deliver_messages()
            
            elif msg[0] == 'ACK':
                # Acknowledgment: ('ACK', ack_sender, original_sender, msg_number, original_timestamp, ack_timestamp)
                _, ack_sender, original_sender, msg_number, original_timestamp, ack_timestamp = msg
                
                # Update Lamport clock
                update_lamport_clock(ack_timestamp)
                
                # Record the acknowledgment
                with acks_lock:
                    acks_received[(original_timestamp, original_sender)].add(ack_sender)
                
                # Try to deliver messages
                try_deliver_messages()
        
        # Write log file
        logFile = open('logfile'+str(myself)+'.log', 'w')
        logFile.writelines(str(delivered_messages))
        logFile.close()
        
        # Send the list of messages to the server for comparison
        print('Sending the list of messages to the server for comparison...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(delivered_messages)
        clientSock.send(msgPack)
        clientSock.close()
        
        # Reset for next round
        global lamport_clock, message_queue, acks_received, delivered_messages, delivery_counter
        lamport_clock = 0
        message_queue = []
        acks_received.clear()
        delivered_messages = []
        delivery_counter = 0
        handShakeCount = 0

        exit(0)

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
            heapq.heappush(message_queue, (timestamp, myself, msgNumber))
        with acks_lock:
            acks_received[(timestamp, myself)].add(myself)
        
        # Try to deliver messages
        try_deliver_messages()
        
        print(f'Sent message {msgNumber} with timestamp {timestamp}')

    # Tell all processes that I have no more messages to send
    for addrToSend in PEERS:
        msg = ('STOP', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))