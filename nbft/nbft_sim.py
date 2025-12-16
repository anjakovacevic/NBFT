from typing import List, Dict, Set
from queue import PriorityQueue
import collections
from .models import Node, Message, MsgType, Vote, RunConfig, RunResult, ConsensusState
from .consistent_hash import ConsistentHashing
from .byzantine import ByzantineBehavior
from .analysis import Analysis

class NBFTSimulator:
    """
    Step 6: NBFT Simulator (Core Task).
    Implements the two-level consensus mechanism:
    1. Intra-group (Members -> Representative)
    2. Inter-group (Between Representatives)
    """
    def __init__(self, config: RunConfig):
        self.config = config
        self.nodes = self._setup_nodes()
        self.ch = ConsistentHashing(self.nodes, config.m)
        self.groups, self.node_group_map = self.ch.form_groups(0) # View 0
        
        # Mark representatives
        self.reps = set()
        for g in self.groups:
            self.nodes[g.representative_id].is_group_representative = True
            self.reps.add(g.representative_id)
            
        self.global_primary_id = self.ch.get_global_primary(0)
        self.nodes[self.global_primary_id].is_global_primary = True
        
        self.msg_queue = PriorityQueue()
        self.global_time = 0.0
        self.logs = []
        
        # node_id -> state dict
        self.node_states = {n.node_id: {} for n in self.nodes}
        self.trace = [] # Store (time, sender, receiver, msg_type)
        
        self.consensus_reached = False
        self.decided_value = None
        self.message_count = 0
        self.phase_counts = collections.defaultdict(int)

    def _setup_nodes(self) -> List[Node]:
        nodes = []
        # Fix: Assign Byzantine nodes from the END of the list to preserve Node 0 (Global Primary)
        byz_indices = set(range(self.config.n - 1, self.config.n - 1 - self.config.actual_byzantine, -1))

        for i in range(self.config.n):
            is_byz = i in byz_indices
            node = Node(
                node_id=i,
                public_key=f"pub_{i}",
                is_byzantine=is_byz,
                byzantine_strategy="silent" if is_byz else "none"
            )
            nodes.append(node)
        return nodes

    def log(self, msg: str):
        self.logs.append(f"[{self.global_time:.2f}] {msg}")

    def send(self, sender: Node, target_id: int, msg: Message):
        # if sender.node_id == target_id: return # Allow self-sending for quorum consistency
        target = self.nodes[target_id]
        
        final_msg = ByzantineBehavior.apply_behavior(sender, msg, target_id)
        if final_msg is None: return

        latency = 0.01 # Simplified
        arrival = self.global_time + latency
        
        # Record trace
        self.trace.append({
            "time": self.global_time,
            "arrival": arrival,
            "sender": sender.node_id,
            "receiver": target_id,
            "type": msg.msg_type.name
        })
        
        # Clone
        cloned = Message(final_msg.msg_type, final_msg.sender_id, final_msg.view, 
                         final_msg.sequence_number, final_msg.digest, final_msg.content)
        
        self.msg_queue.put((arrival, cloned, target_id))
        self.message_count += 1
        self.phase_counts[msg.msg_type.name] += 1

    def run(self) -> RunResult:
        self.log("Starting NBFT simulation")
        
        # Phase 1: Global Primary -> Representatives (Pre-Prepare equivalent)
        # In NBFT, Client -> Primary. Primary -> Reps.
        gp = self.nodes[self.global_primary_id]
        req_msg = Message(MsgType.REP_PRE_PREPARE, gp.node_id, 0, 1, "digest_nbft", "VALUE_Y")
        
        # Multicast to Reps only
        for rep_id in self.reps:
            self.send(gp, rep_id, req_msg)
            
        # Process Queue
        max_steps = 15000
        steps = 0
        
        while not self.msg_queue.empty() and steps < max_steps:
            steps += 1
            t, msg, target_id = self.msg_queue.get()
            self.global_time = t
            self._handle_message(self.nodes[target_id], msg)
            if self.consensus_reached: break
            
        return RunResult(
            success=self.consensus_reached,
            consensus_time=self.global_time,
            total_messages=self.message_count,
            messages_per_phase=dict(self.phase_counts),
            decided_value=self.decided_value,
            logs=self.logs,
            message_trace=self.trace
        )

    def _handle_message(self, node: Node, msg: Message):
        key = (msg.view, msg.sequence_number)
        if key not in self.node_states[node.node_id]:
            self.node_states[node.node_id][key] = {
                "group_votes": set(), # For reps collecting member votes
                "rep_prepare_votes": set(), # For inter-group PBFT
                "rep_commit_votes": set(),
                "state": ConsensusState.IDLE
            }
        
        st = self.node_states[node.node_id][key]
        
        # --- PHASE 1 & 2: Rep Pre-Prepare & Group Pre-Prepare ---
        if msg.msg_type == MsgType.REP_PRE_PREPARE:
            if node.is_group_representative: # check just in case
                # Rep broadcasts to its group members
                # "Algorithm 1" logic
                group_id = self.node_group_map[node.node_id]
                group = self.groups[group_id]
                
                group_msg = Message(MsgType.GROUP_PRE_PREPARE, node.node_id, msg.view, msg.sequence_number, msg.digest, msg.content)
                
                # Send to all members (including self? handled by logic, but usually self-vote is implicit)
                for member_id in group.members:
                    self.send(node, member_id, group_msg)
                    
        # --- PHASE 3: Group Members Vote ---
        elif msg.msg_type == MsgType.GROUP_PRE_PREPARE:
            # Ordinary node validates and sends vote back to Rep
            # "Algorithm 1: Node Decision Broadcast"
            vote_msg = Message(MsgType.GROUP_VOTE, node.node_id, msg.view, msg.sequence_number, msg.digest)
            # Find my rep
            # Simplified: The sender of the GROUP_PRE_PREPARE is the rep
            rep_id = msg.sender_id
            self.send(node, rep_id, vote_msg)
            
        # --- PHASE 4: Rep Collects Votes (Algorithm 2) ---
        elif msg.msg_type == MsgType.GROUP_VOTE:
            if node.is_group_representative:
                st["group_votes"].add(msg.sender_id)
                
                # Calculate Threshold w
                # R = size of group. 
                # Theoretically R = floor((n-1)/m), but we use actual size
                group_id = self.node_group_map[node.node_id]
                current_group_size = self.groups[group_id].size
                w = Analysis.calculate_w(current_group_size)
                # Requirement: Votes >= R - w (Honest majority in group?)
                # Actually paper says: "If collect (R-w) consistent votes"
                
                threshold = current_group_size - w
                
                if len(st["group_votes"]) >= threshold and st["state"] == ConsensusState.IDLE:
                    st["state"] = ConsensusState.PRE_PREPARED # Misnomer, but essentially "Group Consensus Reached"
                    self.log(f"Rep {node.node_id} reached Intra-Group consensus (votes: {len(st['group_votes'])})")
                    
                    # --- PHASE 5: Inter-Group Consensus (Rep-to-Rep PBFT) ---
                    # Send REP_PREPARE to other Reps
                    rep_prep = Message(MsgType.REP_PREPARE, node.node_id, msg.view, msg.sequence_number, msg.digest)
                    for r_id in self.reps:
                        self.send(node, r_id, rep_prep)
                        
        # --- PHASE 5 continued: Inter-Group PBFT ---
        elif msg.msg_type == MsgType.REP_PREPARE:
            if node.is_group_representative:
                st["rep_prepare_votes"].add(msg.sender_id)
                # Threshold E = floor((m-1)/3)
                # Quorum among reps = 2E + 1
                E = Analysis.calculate_E(self.config.m)
                quorum = 2 * E + 1
                
                if len(st["rep_prepare_votes"]) >= quorum and st["state"] != ConsensusState.PREPARED:
                    st["state"] = ConsensusState.PREPARED
                    # Send REP_COMMIT
                    rep_com = Message(MsgType.REP_COMMIT, node.node_id, msg.view, msg.sequence_number, msg.digest)
                    for r_id in self.reps:
                        self.send(node, r_id, rep_com)
                        
        elif msg.msg_type == MsgType.REP_COMMIT:
            if node.is_group_representative:
                st["rep_commit_votes"].add(msg.sender_id)
                E = Analysis.calculate_E(self.config.m)
                quorum = 2 * E + 1
                
                if len(st["rep_commit_votes"]) >= quorum and st["state"] != ConsensusState.COMMITTED:
                    st["state"] = ConsensusState.COMMITTED
                    self.decided_value = "VALUE_Y"
                    self.log(f"Rep {node.node_id} reached Inter-Group consensus")
                    
                    # --- PHASE 6: Broadcast Decision to Group ---
                    final_msg = Message(MsgType.FINAL_DECISION, node.node_id, msg.view, msg.sequence_number, msg.digest)
                    group_id = self.node_group_map[node.node_id]
                    for member_id in self.groups[group_id].members:
                        self.send(node, member_id, final_msg)
                        
        # --- PHASE 6 continued: Members accept decision ---
        elif msg.msg_type == MsgType.FINAL_DECISION:
             self.decided_value = "VALUE_Y"
             self.consensus_reached = True # Global flag for sim termination
             st["state"] = ConsensusState.DECIDED
