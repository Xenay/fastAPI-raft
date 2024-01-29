import time
import requests

class HeartbeatMonitor():
    def __init__(self, nodes, commit_index,leader_id, term, log, next_index):
        self.nodes = nodes
        self.next_index = next_index
        self.commit_index = commit_index
        self.leader_id = leader_id
        self.term = term
        self.log = log
        self.leader_index = commit_index
        # List of nodes in the system

    def send_heartbeats(self):
        while True:
            for follower in self.nodes:
                if follower["state"] != 'leader':
                    self.check_node_status(follower)
                    print("next index is:", self.next_index)
                    self.send_append_entry_to_follower(follower, self.commit_index, self.leader_id, self.term) 
            time.sleep(0.5)  # Example heartbeat interval

    def check_node_status(self, node):
        try:
            print(f"http://{node['ip']}:{node['port']}/heartbeat")
            response = requests.get(f"http://{node['ip']}:{node['port']}/heartbeat")
            if response.status_code == 200:
                print(f"Node {node['ip']}:{node['port']} is up")
            else:
                print(f"Node {node['ip']}:{node['port']} is down")
        except requests.exceptions.RequestException:
                print(f"Node {node['ip']}:{node['port']} is down")
                

    def send_append_entry_to_follower(self, follower, commit_index, leader_id, term, command = None):
        # Prepare the data for the appendEntries RPC
        data = {
            "term": term,
            "leader_id": leader_id,
            "prev_log_index": self.get_prev_log_index(follower),
            "prev_log_term": self.get_prev_log_term(follower, self.log),
            "entries": self.get_entries(follower, self.log),
            "leader_commit": commit_index,
        }
        print("sending: ", data)
        try:
            response = requests.post(f"http://{follower['ip']}:{follower['port']}/append_entries", json=data)
            # Handle the response
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("success"):
                    if follower['port'] != self.leader_id:
                        self.next_index[follower["port"]] = response_data["last_log_index"] + 1
                    #self.commit_index += 1
                elif not response_data.get("success") and response_data.get("error") == "Log index mismatch":
                    print(commit_index)
                    self.decrement_next_index(follower)
                    self.send_append_entry_to_follower(follower, self.next_index[follower["port"]], leader_id, term, command = self.log[self.next_index[follower["port"]]])
 
        except requests.exceptions.RequestException as e:
            print(f"Error contacting node {follower['port']}: {e}")
  
    def send_append_entries(self):
        for node in self.nodes:
            if node['state'] != "leader":  # Exclude self (leader)
                self.send_append_entry_to_follower(node, self.commit_index, self.leader_id, self.term)
        print("cleaning log")
        self.log = []
           
    def get_prev_log_term(self, follower, log):
        prev_log_index = self.get_prev_log_index(follower)
        if prev_log_index < 0 or prev_log_index >= len(log):
            return 0  # There is no previous log, or index out of range
        return log[prev_log_index].term
    
    def get_prev_log_index(self, follower):
        follower_next_index = self.next_index[follower['port']] 
        if follower_next_index == 0: return 0 
        return follower_next_index# Use next_index
        
    def get_entries(self, follower, log):
        follower_next_index = self.next_index[follower['port']]
        # Debug print to check the entries being sent
        entries_to_send = [entry.to_dict() for entry in log[follower_next_index:]]
        
        return entries_to_send
    
    def update_next_index(self, follower, log):
        
        self.next_index[follower['port']] += 1  # Use next_index
        
    def decrement_next_index(self, follower):
        print("indexulja prije", self.next_index)
        self.next_index[follower['port']] = max(0, self.next_index[follower['port']] - 1)  # Use next_index
        print("indexulja poslije", self.next_index)
        
    def handle_log_inconsistency(self, follower, response_data):
        if response_data.get("error") in ["Log term mismatch", "Log index mismatch"]:
        # Decrement next_index for the follower and retry
            self.decrement_next_index(follower)