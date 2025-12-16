import collections
from typing import List, Dict, Set, Optional
from queue import PriorityQueue
from .models import Node, Message, MsgType, RunConfig, RunResult, ConsensusState
from .byzantine import ByzantineBehavior

class PBFTSimulator:
    """
    Step 5: PBFT Baseline Simulator.
    simplified implementation of practical byzantine fault tolerance.
    """
    def __init__(self, config: RunConfig):
        self.config = config
        self.nodes = self._setup_nodes()
        self.msg_queue = PriorityQueue() # (time, message, target)
        self.global_time = 0.0
        self.logs = []
        
        # State tracking per node
        # node_id -> { view -> { seq -> { phase -> count } } }
        self.node_states = {n.node_id: {} for n in self.nodes}
        self.node_logs = {n.node_id: [] for n in self.nodes}
        
        self.consensus_reached = False
        self.decided_value = None
        self.message_count = 0
        self.phase_counts = collections.defaultdict(int)

    def _setup_nodes(self) -> List[Node]:
        nodes = []
        # Fix: Assign Byzantine nodes from the END of the list to preserve Node 0 (Primary)
        # as honest for as long as possible (simulating "Primary is honest" or until rotation)
        byz_indices = set(range(self.config.n - 1, self.config.n - 1 - self.config.actual_byzantine, -1))
        
        for i in range(self.config.n):
            is_byz = i in byz_indices
            node = Node(
                node_id=i,
                public_key=f"pub_{i}",
                is_byzantine=is_byz,
                byzantine_strategy="silent" if is_byz else "none" # Default bad strategy
            )
            nodes.append(node)
        return nodes

    def log(self, msg: str):
        self.logs.append(f"[{self.global_time:.2f}] {msg}")

    def send_multicast(self, sender: Node, msg: Message):
        for target in self.nodes:
            # if target.node_id == sender.node_id: continue # Allow self-sending
            self._schedule_message(sender, target, msg)

    def _schedule_message(self, sender: Node, target: Node, msg: Message):
        # Apply Byzantine behavior
        final_msg = ByzantineBehavior.apply_behavior(sender, msg, target.node_id)
        if final_msg is None:
            return # Drop message
        
        # Latency model
        latency = 0.01 if self.config.latency_profile == "constant" else 0.01 # simplified
        arrival_time = self.global_time + latency
        
        # Use python's struct for "copy" sim or just new object to avoid ref issues
        # Simple clone via constructor for safety
        cloned_msg = Message(
            msg_type=final_msg.msg_type,
            sender_id=final_msg.sender_id,
            view=final_msg.view,
            sequence_number=final_msg.sequence_number,
            digest=final_msg.digest,
            content=final_msg.content
        )
        
        self.msg_queue.put((arrival_time, cloned_msg, target.node_id))
        self.message_count += 1
        self.phase_counts[msg.msg_type.name] += 1

    def run(self) -> RunResult:
        self.log("Starting PBFT simulation")
        
        # 1. Client Request (Simulated)
        primary_id = 0 # Simple view 0
        req_msg = Message(MsgType.PRE_PREPARE, primary_id, 0, 1, "digest_req", "VALUE_X")
        
        # Primary Multicasts PRE-PREPARE
        primary = self.nodes[primary_id]
        
        # CRITICAL FIX: If Primary is Silent Byzantine, it won't send anything.
        # We simulate the ATTEMPT to send. The _schedule_message filter will handle the silence.
        self.send_multicast(primary, req_msg)
        
        # Process Queue
        max_steps = 10000
        steps = 0
        
        while not self.msg_queue.empty() and steps < max_steps:
            steps += 1
            t, msg, target_id = self.msg_queue.get()
            self.global_time = t
            
            self._handle_message(self.nodes[target_id], msg)
            
            if self.consensus_reached:
                break
                
        return RunResult(
            success=self.consensus_reached,
            consensus_time=self.global_time,
            total_messages=self.message_count,
            messages_per_phase=dict(self.phase_counts),
            decided_value=self.decided_value,
            logs=self.logs,
            message_trace=[]
        )

    def _handle_message(self, node: Node, msg: Message):
        if node.is_byzantine and node.byzantine_strategy == "silent":
             # Silent nodes might receive but don't process/act (simplified)
             return
             
        # Threshold calculation: 2f + 1
        f = (self.config.n - 1) // 3
        quorum = 2 * f + 1
        
        # Initialize state storage for this view/seq if needed
        state_key = (msg.view, msg.sequence_number)
        if state_key not in self.node_states[node.node_id]:
            self.node_states[node.node_id][state_key] = {
                "pre_prepare": False,
                "prepare_votes": set(),
                "commit_votes": set(),
                "state": ConsensusState.IDLE
            }
        
        st = self.node_states[node.node_id][state_key]
        
        # Logic Flow
        if msg.msg_type == MsgType.PRE_PREPARE:
            if st["state"] == ConsensusState.IDLE:
                st["state"] = ConsensusState.PRE_PREPARED
                st["pre_prepare"] = True
                # Broadcast PREPARE
                reply = Message(MsgType.PREPARE, node.node_id, msg.view, msg.sequence_number, msg.digest)
                self.send_multicast(node, reply)
                
        elif msg.msg_type == MsgType.PREPARE:
            st["prepare_votes"].add(msg.sender_id)
            if len(st["prepare_votes"]) >= quorum and st["state"] == ConsensusState.PRE_PREPARED:
                st["state"] = ConsensusState.PREPARED
                # Broadcast COMMIT
                commit = Message(MsgType.COMMIT, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content)
                self.send_multicast(node, commit)
                
        elif msg.msg_type == MsgType.COMMIT:
            st["commit_votes"].add(msg.sender_id)
            if len(st["commit_votes"]) >= quorum and st["state"] == ConsensusState.PREPARED:
                st["state"] = ConsensusState.COMMITTED
                # Execute/Decide
                # For simulation, if >= f+1 honest nodes decide, we consider it global success
                # But here we track if ANY honest node decides
                self.decided_value = "VALUE_X" # Simplified
                if not node.is_byzantine:
                    self.consensus_reached = True
                    self.log(f"Node {node.node_id} reached consensus on {self.decided_value}")
