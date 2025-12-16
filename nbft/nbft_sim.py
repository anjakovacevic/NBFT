from typing import List, Dict, Set
import asyncio
import time
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
        self.global_time = 0.0
        self.logs = []
        
        self.nodes = self._setup_nodes()
        self.ch = ConsistentHashing(self.nodes, config.m)
        # 2. Form Groups & Identify Reps for View 0
        self._setup_view(0)
        
        # 4. Inject Byzantine Behavior (Targeting Reps)
        # Note: Strategies might need re-evaluating on view change if Reps change.
        # For now, we keep static byzantine flags but their 'role-based' behavior might shift.
        import random
        
        # Determine potential targets (just random sampling from population)
        # We do this once globaly.
        # ... (Existing Byzantine injection logic seems fine, it relies on self.reps which is now dynamic)
        
        # Re-apply strategy based on INITIAL view. 
        # Ideally we re-check this every view change.
        self._apply_byzantine_strategies()
        
        # node_id -> state dict
        self.node_states = {n.node_id: {} for n in self.nodes}
        self.trace = [] # Store (time, sender, receiver, msg_type)
        
        self.consensus_reached = False
        self.decided_value = None
        self.message_count = 0
        self.phase_counts = collections.defaultdict(int)

    def _setup_nodes(self) -> List[Node]:
        nodes = []
        import random
        # Randomly select Byzantine nodes from the entire population
        byz_indices = set(random.sample(range(self.config.n), self.config.actual_byzantine))

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

    def _setup_view(self, view_id: int):
        """
        Configures groups, representatives, and primary for a specific view.
        """
        # Form Groups
        self.groups, self.node_group_map = self.ch.form_groups(view_id)
        
        # Identify Reps
        self.reps = set()
        # Reset Node roles for clean slate
        for n in self.nodes:
            n.is_group_representative = False
            n.is_global_primary = False
            
        for g in self.groups:
            self.nodes[g.representative_id].is_group_representative = True
            self.reps.add(g.representative_id)
            
        self.global_primary_id = self.ch.get_global_primary(view_id)
        self.nodes[self.global_primary_id].is_global_primary = True
        self.log(f"View {view_id} Setup: Primary={self.global_primary_id}, Reps={self.reps}")

    def _apply_byzantine_strategies(self):
        # Post-process: If a randomly chosen Byzantine node happens to be a Rep,
        # set its strategy to "bad_aggregator" to trigger the Watchdog logic.
        for rep_id in self.reps:
            if self.nodes[rep_id].is_byzantine:
                self.nodes[rep_id].byzantine_strategy = "bad_aggregator"

    def log(self, msg: str):
        self.logs.append(f"[{self.global_time:.2f}] {msg}")

    def send(self, sender: Node, target_id: int, msg: Message):
        asyncio.create_task(self._async_send(sender, target_id, msg))

    async def _async_send(self, sender: Node, target_id: int, msg: Message):
        # Special case: Sending to Client (ID -1)
        if target_id == -1:
            latency = 0.001
            await asyncio.sleep(latency)
            current_time = time.time() - self.start_time
            self.trace.append({
                "time": current_time - latency,
                "arrival": current_time,
                "sender": sender.node_id,
                "receiver": -1,
                "type": msg.msg_type.name
            })
            return

        target = self.nodes[target_id]
        
        final_msg = ByzantineBehavior.apply_behavior(sender, msg, target_id)
        if final_msg is None: return

        # Latency Emulation
        latency = 0.001 
        await asyncio.sleep(latency)
        
        current_time = time.time() - self.start_time
        arrival = current_time
        
        # Record trace
        self.trace.append({
            "time": current_time - latency, 
            "arrival": arrival,
            "sender": sender.node_id,
            "receiver": target_id,
            "type": msg.msg_type.name
        })
        
        # Sign the message
        # We sign the digest as the unique content representative
        signature = Message.sign(final_msg.digest, sender.node_id)
        
        cloned = Message(final_msg.msg_type, final_msg.sender_id, final_msg.view, 
                         final_msg.sequence_number, final_msg.digest, final_msg.content, 
                         signature=signature)
        
        self.message_count += 1
        self.phase_counts[msg.msg_type.name] += 1
        
        self._handle_message(target, cloned)

    async def run(self) -> RunResult:
        self.log("Starting NBFT simulation (Async)")
        self.consensus_event = asyncio.Event()
        self.start_time = time.time()
        
        current_view = 0
        max_views = 3 # Try up to View 2
        
        while current_view < max_views and not self.consensus_reached:
            # Setup for current view
            if current_view > 0:
                self._setup_view(current_view)
                self._apply_byzantine_strategies() # Re-apply strategies for new reps
                
                # Simulate VIEW_CHANGE traffic
                # All nodes broadcast VIEW_CHANGE to everyone (simplified trace)
                # In reality: Node i sends VIEW_CHANGE to new Primary
                new_prim = self.nodes[self.global_primary_id]
                for n in self.nodes:
                    # Just add ONE trace entry per node to represent the broadcast
                    # latency = 0.001
                    # self.trace.append(...) 
                    # Use _async_send? No, might trigger handlers. Just log for now or direct trace.
                    pass
                
                # New Primary announces NEW_VIEW
                # self.send(new_prim, -1, Message(MsgType.NEW_VIEW, ...)) # trace only
                self.log(f"--- STARTING VIEW {current_view} ---")
                # Reset consensus event for the new view
                self.consensus_event = asyncio.Event()
                # Clear node states for the new view to avoid stale data
                self.node_states = {n.node_id: {} for n in self.nodes}


            # Start Consensus Protocol for this View
            gp = self.nodes[self.global_primary_id]
            req_msg = Message(MsgType.REP_PRE_PREPARE, gp.node_id, current_view, 1, "digest_nbft", "VALUE_Y")
            
            # Global Primary sends to Reps
            for rep_id in self.reps:
                self.send(gp, rep_id, req_msg)
                
            try:
                # Wait for consensus or timeout
                # 2.0 seconds timeout per view
                await asyncio.wait_for(self.consensus_event.wait(), timeout=2.0)
                if self.consensus_reached:
                    self.log(f"Consensus reached in View {current_view}")
                    break
                    
            except asyncio.TimeoutError:
                self.log(f"View {current_view} TIMEOUT. Initiating View Change...")
                
                # 1. Broadcast VIEW_CHANGE
                # Every node broadcasts 'VIEW_CHANGE'
                vc_msg = Message(MsgType.VIEW_CHANGE, 0, current_view + 1, 0, "params")
                for n in self.nodes:
                     # Simulate broadcast to the 'Next Primary' (simplified)
                     # In full PBFT, this goes to everyone.
                     # We'll just trace 1 msg per node to indicate traffic
                     next_prim_id = self.ch.get_global_primary(current_view + 1)
                     self.send(n, next_prim_id, Message(MsgType.VIEW_CHANGE, n.node_id, current_view + 1, 0, ""))
                
                # 2. Wait a bit for View Change processing
                await asyncio.sleep(0.1)
                
                current_view += 1
                if current_view >= max_views:
                    self.log("Max views reached. Simulation failed.")

        duration = time.time() - self.start_time
        
        return RunResult(
            success=self.consensus_reached,
            consensus_time=duration,
            total_messages=self.message_count,
            messages_per_phase=dict(self.phase_counts),
            decided_value=self.decided_value,
            logs=self.logs,
            message_trace=self.trace,
            byzantine_nodes=[n.node_id for n in self.nodes if n.is_byzantine]
        )

    def _handle_message(self, node: Node, msg: Message):
        # 0. SECURITY CHECK: Verify Signature
        if msg.signature:
            is_valid = Message.verify(msg.digest, msg.signature, msg.sender_id)
            if not is_valid:
                return # Drop message
                
        key = (msg.view, msg.sequence_number)
        if key not in self.node_states[node.node_id]:
            self.node_states[node.node_id][key] = {
                "group_votes": set(), # For reps collecting member votes
                "rep_prepare_votes": set(), # For inter-group PBFT
                "rep_commit_votes": set(),
                "flagged_groups": set(), # Store group_ids with active alarms
                "state": ConsensusState.IDLE
            }
        
        st = self.node_states[node.node_id][key]
        
        # PHASE 1 & 2: Rep Pre-Prepare & Group Pre-Prepare 
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
                    
        # PHASE 3: Group Members Vote 
        elif msg.msg_type == MsgType.GROUP_PRE_PREPARE:
            # Ordinary node validates and sends vote back to Rep
            # "Algorithm 1: Node Decision Broadcast"
            vote_msg = Message(MsgType.GROUP_VOTE, node.node_id, msg.view, msg.sequence_number, msg.digest)
            # Find my rep
            # Simplified: The sender of the GROUP_PRE_PREPARE is the rep
            rep_id = msg.sender_id
            self.send(node, rep_id, vote_msg)
            
        # PHASE 4: Rep Collects Votes (Algorithm 2) 
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
                    
                    # 1. NEW: Broadcast Result sent to Members for verification
                    group_res = Message(MsgType.GROUP_RESULT, node.node_id, msg.view, msg.sequence_number, msg.digest)
                    group_id = self.node_group_map[node.node_id]
                    for member_id in self.groups[group_id].members:
                        if member_id != node.node_id:
                            self.send(node, member_id, group_res)

                    # 2. Proceed to Inter-Group Consensus (Rep-to-Rep PBFT)
                    rep_prep = Message(MsgType.REP_PREPARE, node.node_id, msg.view, msg.sequence_number, msg.digest)
                    for r_id in self.reps:
                        self.send(node, r_id, rep_prep)

        # Verify the result
        elif msg.msg_type == MsgType.GROUP_RESULT:
            if not node.is_byzantine:
                # Watchdog Check
                is_valid = True 
                if msg.digest == "CORRUPTED_AGGREGATE":
                    is_valid = False
                    
                if is_valid:
                    # self.log(f"Node {node.node_id} verified Rep decision: OK")
                    pass
                else:
                    self.log(f"ALARM! Node {node.node_id} detected fraud by Rep {msg.sender_id}")
                    alarm_msg = Message(MsgType.ALARM, node.node_id, msg.view, msg.sequence_number, msg.digest, "Fraud detected")
                    # Broadcast alarm to everyone
                    for n in self.nodes:
                         self.send(node, n.node_id, alarm_msg)

        # ALARM Handling
        elif msg.msg_type == MsgType.ALARM:
            # If ALARM received, we must identify which group it came from
            sender_group_id = self.node_group_map.get(msg.sender_id, -1)
            
            if sender_group_id != -1:
                st["flagged_groups"].add(sender_group_id)
                self.log(f"Node {node.node_id} flagged Group {sender_group_id} due to ALARM from {msg.sender_id}")
            else:
                 self.log(f"Node {node.node_id} received ALARM from {msg.sender_id} (Unknown Group)")
        # PHASE 5 continued: Inter-Group PBFT 
        elif msg.msg_type == MsgType.REP_PREPARE:
            if node.is_group_representative:
                sender_group = self.node_group_map.get(msg.sender_id, -1)
                
                # Exclusion Logic: Ignore votes from flagged groups
                if sender_group in st["flagged_groups"]:
                    self.log(f"Rep {node.node_id} IGNORED Prep vote from Rep {msg.sender_id} (Group {sender_group} flagged)")
                    # ignoring the vote
                    pass
                else:
                    # count the vote
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
                sender_group = self.node_group_map.get(msg.sender_id, -1)

                # Exclusion Logic: Ignore votes from flagged groups
                if sender_group in st["flagged_groups"]:
                    self.log(f"Rep {node.node_id} IGNORED Commit vote from Rep {msg.sender_id} (Group {sender_group} flagged)")
                    # ignoring the vote
                    pass
                else:
                    st["rep_commit_votes"].add(msg.sender_id)

                E = Analysis.calculate_E(self.config.m)
                quorum = 2 * E + 1
                
                if len(st["rep_commit_votes"]) >= quorum and st["state"] != ConsensusState.COMMITTED:
                    st["state"] = ConsensusState.COMMITTED
                    self.decided_value = "VALUE_Y"
                    self.log(f"Rep {node.node_id} reached Inter-Group consensus")
                    
                    # Send Reply to Client
                    self.send(node, -1, Message(MsgType.REPLY, node.node_id, msg.view, msg.sequence_number, msg.digest))
                    
                    # PHASE 6: Broadcast Decision to Group 
                    final_msg = Message(MsgType.FINAL_DECISION, node.node_id, msg.view, msg.sequence_number, msg.digest)
                    group_id = self.node_group_map[node.node_id]
                    for member_id in self.groups[group_id].members:
                        self.send(node, member_id, final_msg)
                        
        # PHASE 6 continued: Members accept decision 
        elif msg.msg_type == MsgType.FINAL_DECISION:
             self.decided_value = "VALUE_Y"
             self.consensus_reached = True # Global flag for sim termination
             st["state"] = ConsensusState.DECIDED
             self.consensus_event.set()
             
             # Send Reply to Client
             self.send(node, -1, Message(MsgType.REPLY, node.node_id, msg.view, msg.sequence_number, msg.digest))
