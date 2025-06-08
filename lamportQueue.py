import heapq
import threading

class LamportMessageQueue:
    """
    Thread-safe priority queue for Lamport's total ordering algorithm.
    Messages are ordered by (timestamp, sender_id) for tie-breaking.
    """
    
    def __init__(self):
        self.queue = []
        self.lock = threading.Lock()
        self.message_set = set()  # To avoid duplicates
    
    def push(self, timestamp, sender_id, message):
        """Add a message to the queue"""
        with self.lock:
            key = (timestamp, sender_id, message)
            if key not in self.message_set:
                heapq.heappush(self.queue, (timestamp, sender_id, message))
                self.message_set.add(key)
    
    def peek(self):
        """Look at the top message without removing it"""
        with self.lock:
            if self.queue:
                return self.queue[0]
            return None
    
    def pop(self):
        """Remove and return the top message"""
        with self.lock:
            if self.queue:
                item = heapq.heappop(self.queue)
                self.message_set.remove(item)
                return item
            return None
    
    def empty(self):
        """Check if queue is empty"""
        with self.lock:
            return len(self.queue) == 0
    
    def size(self):
        """Get queue size"""
        with self.lock:
            return len(self.queue)
    
    def clear(self):
        """Clear the queue"""
        with self.lock:
            self.queue = []
            self.message_set.clear()