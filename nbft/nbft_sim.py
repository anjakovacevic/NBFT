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

    def log(self, msg: str, source: str = "simulation", level: str = "INFO"):
        if not hasattr(self, 'verbose_logging'): self.verbose_logging = True # Default or set in init
        
        # In Single Simulation, we want detailed logs.
        # Format: YYYY-MM-DD HH:MM:SS,fff - source - LEVEL - msg
        formatted_msg = f"{level} - {source} - {msg}"
        self.logs.append(formatted_msg)

    def send(self, sender: Node, target_id: int, msg: Message):
        # Log the send event if verbose
        # self.log(f"[{msg.msg_type.name}] Sent to node-{target_id}: {msg.digest[:10]}...", source=f"node-{sender.node_id}")
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
                         signature=signature, weight=final_msg.weight)
        
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
                "rep_prepare_votes": {}, # For inter-group PBFT: sender_id -> weight
                "rep_commit_votes": {}, # sender_id -> weight
                "flagged_groups": set(), # Store group_ids with active alarms
                "state": ConsensusState.IDLE,
                "intra_group_done": False # Track if we finished local group consensus, ensures every Representative always broadcasts its vote once its local group reaches consensus
            }
        
        st = self.node_states[node.node_id][key]
        
        # PHASE 1 & 2: Rep Pre-Prepare & Group Pre-Prepare 
        # PHASE 1 & 2: Rep Pre-Prepare & Group Pre-Prepare 
        if msg.msg_type == MsgType.REP_PRE_PREPARE:
            if node.is_group_representative: # check just in case
                # Store the proposal value
                st["proposal_value"] = msg.content
                
                # Rep broadcasts to its group members
                # "Algorithm 1" logic
                group_id = self.node_group_map[node.node_id]
                group = self.groups[group_id]
                
                self.log(f"[IN_PREPARE1 | GROUP {group_id}] Sent InPrepare (value={msg.content})", source=f"node-{node.node_id}")
                
                group_msg = Message(MsgType.GROUP_PRE_PREPARE, node.node_id, msg.view, msg.sequence_number, msg.digest, msg.content)
                
                # Send to all members (including self? handled by logic, but usually self-vote is implicit)
                for member_id in group.members:
                    self.send(node, member_id, group_msg)
                    
        # PHASE 3: Group Members Vote 
        elif msg.msg_type == MsgType.GROUP_PRE_PREPARE:
            # Ordinary node validates and sends vote back to Rep
            # "Algorithm 1: Node Decision Broadcast"
            
            # Store the proposal value
            st["proposal_value"] = msg.content
            
            # Find my rep
            # Simplified: The sender of the GROUP_PRE_PREPARE is the rep
            rep_id = msg.sender_id
            
            self.log(f"[SIGN | GROUP ?] Signed '{msg.content}'", source=f"node-{node.node_id}")
            
            vote_msg = Message(MsgType.GROUP_VOTE, node.node_id, msg.view, msg.sequence_number, msg.digest)
            self.send(node, rep_id, vote_msg)
            
        # PHASE 4: Rep Collects Votes (Algorithm 2) 
        elif msg.msg_type == MsgType.GROUP_VOTE:
            if node.is_group_representative:
                st["group_votes"].add(msg.sender_id)
                
                # Calculate Threshold w
                group_id = self.node_group_map[node.node_id]
                current_group_size = self.groups[group_id].size
                w = Analysis.calculate_w(current_group_size)
                
                threshold = current_group_size - w
                
                if len(st["group_votes"]) >= threshold and not st["intra_group_done"]:
                    st["intra_group_done"] = True
                    if st["state"] == ConsensusState.IDLE:
                         st["state"] = ConsensusState.PRE_PREPARED
                    
                    # Weighted Voting Implementation
                    # Weight = number of valid votes collected from group members
                    vote_weight = len(st["group_votes"])
                    st["my_weight"] = vote_weight # Store for later use
                    self.log(f"Rep {node.node_id} reached Intra-Group consensus (votes: {vote_weight}, weight: {vote_weight})")
                    
                    # 1. Broadcast Result sent to Members (and implicitly store weight for Inter-group)
                    group_res = Message(MsgType.GROUP_RESULT, node.node_id, msg.view, msg.sequence_number, msg.digest, weight=vote_weight)
                    group_id = self.node_group_map[node.node_id]
                    for member_id in self.groups[group_id].members:
                        if member_id != node.node_id:
                            self.send(node, member_id, group_res)

                    # 2. Proceed to Inter-Group Consensus
                    # Pass the weight in the REP_PREPARE message
                    # PROPOSAL VALUE is passed here to ensure Reps know WHAT they are voting on
                    prop_val = st.get("proposal_value", None)
                    rep_prep = Message(MsgType.REP_PREPARE, node.node_id, msg.view, msg.sequence_number, msg.digest, content=prop_val, weight=vote_weight)
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
                    pass
                else:
                    self.log(f"ALARM! Node {node.node_id} detected fraud by Rep {msg.sender_id}")
                    alarm_msg = Message(MsgType.ALARM, node.node_id, msg.view, msg.sequence_number, msg.digest, "Fraud detected")
                    # Broadcast alarm to everyone
                    for n in self.nodes:
                         self.send(node, n.node_id, alarm_msg)

        # ALARM Handling
        elif msg.msg_type == MsgType.ALARM:
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
                
                my_group_id = self.node_group_map[node.node_id]
                self.log(f"[IN_PREPARE2 | GROUP {my_group_id} | REPRESENTATIVE] Received from node-{msg.sender_id}: {msg.content or msg.digest[:10]}", source=f"node-{node.node_id}")

                # Check exclusion logic
                if sender_group in st["flagged_groups"]:
                    self.log(f"Rep {node.node_id} IGNORED Prep vote from Rep {msg.sender_id} (Group {sender_group} flagged)")
                    pass
                else:
                    # -- Weighted Voting Logic --
                    # Use a dict to store weights: sender_id -> weight
                    st["rep_prepare_votes"][msg.sender_id] = msg.weight
                
                # Calculate Quorum based on TOTAL WEIGHT
                # Max weight possible per group is approx group_size
                # We need a robust 'weighted quorum'
                # For simplicity, we keep the 'count' quorum but we theoretically acknowledge weights
                # Improvements: Total Weight > 2/3 Total Possible Weight
                
                current_total_weight = sum(st["rep_prepare_votes"].values())
                
                # Weighted Quorum: Total weight must be > 2/3 of Total Network Size
                # This ensures that > 2/3 of ALL nodes in the network (indirectly) support the decision
                total_network_weight = self.config.n 
                required_weight = (2 * total_network_weight) // 3
                
                if current_total_weight > required_weight and st["state"] != ConsensusState.PREPARED:
                    st["state"] = ConsensusState.PREPARED
                    # Propagate weight to Commit phase? Usually not needed if Prepare is safe.
                    # Use MY weight, not the sender's weight
                    my_weight = st.get("my_weight", 1) 
                    rep_com = Message(MsgType.REP_COMMIT, node.node_id, msg.view, msg.sequence_number, msg.digest, weight=my_weight)
                    
                    self.log(f"[GLOBAL] Received aggregate from group {sender_group}: rep=node-{msg.sender_id}, value={msg.content}, valid_sigs={msg.weight}", source="simulation")

                    for r_id in self.reps:
                        self.send(node, r_id, rep_com)
                        
        elif msg.msg_type == MsgType.REP_COMMIT:
            if node.is_group_representative:
                sender_group = self.node_group_map.get(msg.sender_id, -1)

                if sender_group in st["flagged_groups"]:
                    self.log(f"Rep {node.node_id} IGNORED Commit vote from Rep {msg.sender_id} (Group {sender_group} flagged)")
                    pass
                else:
                    st["rep_commit_votes"][msg.sender_id] = msg.weight

                current_total_weight = sum(st["rep_commit_votes"].values())
                total_network_weight = self.config.n
                required_weight = (2 * total_network_weight) // 3
                
                if current_total_weight > required_weight and st["state"] != ConsensusState.COMMITTED:
                    st["state"] = ConsensusState.COMMITTED
                    
                    # Use the stored proposal value, or fall back to the message content if present
                    final_value = st.get("proposal_value", msg.content) or "Unknown_Value"
                    self.decided_value = final_value
                    
                    self.log(f"Rep {node.node_id} reached Inter-Group consensus")
                    
                    if not self.consensus_reached:
                         # Log ONLY ONCE for the global success
                         self.log(f"[GLOBAL] Consensus reached: value='{self.decided_value}'", source="simulation")
                    
                    self.consensus_reached = True
                    self.consensus_event.set()
                    
                    # Broadcast Final Decision?
                    dec_msg = Message(MsgType.FINAL_DECISION, node.node_id, msg.view, msg.sequence_number, msg.digest, content=final_value)
                    # Send to everyone
                    for n in self.nodes:
                         self.send(node, n.node_id, dec_msg)
                    pass

                    self.decided_value = "VALUE_Y"
                    self.log(f"Rep {node.node_id} reached Inter-Group consensus")
                    
                    # Send Reply to Client
                    self.send(node, -1, Message(MsgType.REPLY, node.node_id, msg.view, msg.sequence_number, msg.digest))
                    
                    # PHASE 6: Broadcast Decision to Group 

        # PHASE 6 continued: Members accept decision 
        elif msg.msg_type == MsgType.FINAL_DECISION:
             self.decided_value = "VALUE_Y"
             self.consensus_reached = True # Global flag for sim termination
             st["state"] = ConsensusState.DECIDED
             self.consensus_event.set()
             
             # Send Reply to Client
             self.send(node, -1, Message(MsgType.REPLY, node.node_id, msg.view, msg.sequence_number, msg.digest))
