from typing import List
import asyncio
import time
import collections
from .models import Node, Message, MsgType, RunConfig, RunResult, ConsensusState
from .consistent_hash import ConsistentHashing
from .byzantine import ByzantineBehavior
from .analysis import Analysis
import random

class NBFTSimulator:
    """
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
        self.previous_hash = "0" * 64
        self.prev_primary_id = None
        # Form Groups & Identify Reps for View 0
        self._setup_view(0)
        
        # Inject Byzantine Behavior (Targeting Reps)        
        # Re-apply strategy based on INITIAL view. 
        self._apply_byzantine_strategies()
        
        # node_id -> state dict
        self.node_states = {n.node_id: {} for n in self.nodes}
        self.trace = [] # Store (time, sender, receiver, msg_type)
        
        self.consensus_reached = False
        self.decided_value = None
        self.message_count = 0
        self.phase_counts = collections.defaultdict(int)
        self.background_tasks = set()

    def _setup_nodes(self) -> List[Node]:
        nodes = []
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
        self.groups, self.node_group_map, self.global_primary_id = self.ch.form_groups(
            view_id, self.previous_hash, self.prev_primary_id
        )
        
        # Identify Reps
        self.reps = set()
        # Reset Node roles for clean slate
        for n in self.nodes:
            n.is_group_representative = False
            n.is_global_primary = False
            
        for g in self.groups:
            self.nodes[g.representative_id].is_group_representative = True
            self.reps.add(g.representative_id)
            
        self.nodes[self.global_primary_id].is_global_primary = True
        self.prev_primary_id = self.global_primary_id  # Store for next view setup if needed
        self.log(f"View {view_id} Setup: Primary={self.global_primary_id}, Reps={self.reps}")

    def _apply_byzantine_strategies(self):
        # Post-process: If a randomly chosen Byzantine node happens to be a Rep,
        # set its strategy to "bad_aggregator" to trigger the Watchdog logic.
        for rep_id in self.reps:
            if self.nodes[rep_id].is_byzantine:
                self.nodes[rep_id].byzantine_strategy = "bad_aggregator"

    def log(self, msg: str, source: str = "simulation", level: str = "INFO"):
        if not hasattr(self, 'verbose_logging'): self.verbose_logging = True # Default or set in init
        
        # In Single Simulation-> detailed logs.
        formatted_msg = f"{level} - {source} - {msg}"
        self.logs.append(formatted_msg)

    def send(self, sender: Node, target_id: int, msg: Message):
        # Log the send event
        # self.log(f"[{msg.msg_type.name}] Sent to node-{target_id}: {msg.digest[:10]}...", source=f"node-{sender.node_id}")
        task = asyncio.create_task(self._async_send(sender, target_id, msg))
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

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
        
        # Sign the digest as the unique content representative
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
                # All nodes broadcast VIEW_CHANGE to everyone
                new_prim = self.nodes[self.global_primary_id]
                for n in self.nodes:
                    # Just add ONE trace entry per node to represent the broadcast
                    # latency = 0.001
                    # self.trace.append(...)
                    pass
                
                # New Primary announces NEW_VIEW
                # self.send(new_prim, -1, Message(MsgType.NEW_VIEW, ...)) # trace only
                self.log(f" STARTING VIEW {current_view}")
                # Reset consensus event for the new view
                self.consensus_event = asyncio.Event()
                # Clear node states for the new view to avoid stale data
                self.node_states = {n.node_id: {} for n in self.nodes}


            # Start Consensus Protocol for this View
            gp = self.nodes[self.global_primary_id]
            
            # PHASE: preprepare1
            # 1. Client sends REQUEST to Global Primary
            client_req = Message(MsgType.REQUEST, -1, current_view, 1, "digest_nbft", "VALUE_Y")
            self.send(Node(-1, ""), self.global_primary_id, client_req)
                
            try:
                # Wait for consensus or timeout
                # Customer terminal threshold: (n-1)/2 + 1
                await asyncio.wait_for(self.consensus_event.wait(), timeout=3.0)
                if self.consensus_reached:
                    client_threshold = ((self.config.n - 1) // 2) + 1
                    self.log(f"Consensus reached (Client Threshold: {client_threshold} replies received)")
                    break
            except asyncio.TimeoutError:
                self.log(f"View {current_view} TIMEOUT.")
                current_view += 1
                if current_view < max_views:
                    self.log(f" - STARTING VIEW {current_view}  ")

        if self.consensus_reached:
            # Short grace period to let final decision/reply messages finish tracing
            await asyncio.sleep(0.1)

        # Cleanup: Cancel any remaining background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)

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

    async def _async_send_from_client(self, target_id: int, msg: Message):
        """Special helper to simulate client broadcast in preprepare1."""
        latency = 0.001
        await asyncio.sleep(latency)
        current_time = time.time() - self.start_time
        self.trace.append({
            "time": current_time - latency,
            "arrival": current_time,
            "sender": -1,
            "receiver": target_id,
            "type": msg.msg_type.name
        })
        self._handle_message(self.nodes[target_id], msg)

    def _handle_message(self, node: Node, msg: Message):
        key = (msg.view, msg.sequence_number)
        if key not in self.node_states[node.node_id]:
            self.node_states[node.node_id][key] = {
                "in_prepare1_votes": set(), # Rep collecting from members
                "out_prepare_votes": {},    # Rep collecting from other nodes (NodeID -> Weight)
                "processed_sources": set(), # Prevent double counting from same groups
                "commit_votes": set(),      # Node 0 collecting from Reps
                "state": ConsensusState.IDLE,
                "intra_group_done": False,
                "inter_group_done": False,
                "watchdog_triggered": False,
                "proposal_value": None
            }
        
        st = self.node_states[node.node_id][key]
        
        # 0. REQUEST (Client -> Global Primary)
        if msg.msg_type == MsgType.REQUEST:
            if node.node_id == self.global_primary_id:
                self.log(f"[REQUEST] Global Primary received client request. Broadcasting PREPREPARE1 to all nodes.", source=f"node-{node.node_id}")
                st["proposal_value"] = msg.content
                broadcast_msg = Message(MsgType.PREPREPARE1, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content)
                for other_node in self.nodes:
                    self.send(node, other_node.node_id, broadcast_msg)

        # 1. preprepare1 (Global Primary -> All Nodes)
        elif msg.msg_type == MsgType.PREPREPARE1:
            st["proposal_value"] = msg.content
            
            # Every node (if in a group) sends in-prepare1 to its group representative
            gid = self.node_group_map.get(node.node_id, -1)
            if gid != -1:
                rep_id = self.groups[gid].representative_id
                self.log(f"[preprepare1] Received broadcast. Sending in-prepare1 to Rep {rep_id}", source=f"node-{node.node_id}")
                in_prep1 = Message(MsgType.IN_PREPARE1, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content)
                self.send(node, rep_id, in_prep1)
                
                # Algorithm 1: Watchdog Timer
                # Start a watchdog to wait for in-prepare2 from the Representative
                watchdog_task = asyncio.create_task(self._watchdog_timer(node, msg, rep_id))
                self.background_tasks.add(watchdog_task)
                watchdog_task.add_done_callback(self.background_tasks.discard)
            else:
                self.log(f"[preprepare1] Global Primary {node.node_id} skipping intra-group step.", source=f"node-{node.node_id}")

        # 2. in-prepare1 (Node -> Representative)
        elif msg.msg_type == MsgType.IN_PREPARE1:
            if node.is_group_representative:
                st["in_prepare1_votes"].add(msg.sender_id)
                st["proposal_value"] = msg.content
                
                group_id = self.node_group_map[node.node_id]
                group_size = self.groups[group_id].size
                threshold = group_size - Analysis.calculate_w(group_size)
                
                if len(st["in_prepare1_votes"]) >= threshold and not st["intra_group_done"]:
                    # Threshold Vote-Counting Model
                    # Reached FULL local consensus
                    st["intra_group_done"] = True
                    vote_weight = group_size
                    self.log(f"[in-prepare1] Group {group_id} reached FULL threshold. Weight={vote_weight}", source=f"node-{node.node_id}")
                    
                    # in-prepare2: Representative -> All group nodes (Confirmation)
                    in_prep2 = Message(MsgType.IN_PREPARE2, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content)
                    for member_id in self.groups[group_id].members:
                        self.send(node, member_id, in_prep2)
                    
                    # out-prepare: Representatives cross-talk with assigned weight
                    out_prep = Message(MsgType.OUT_PREPARE, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content, weight=vote_weight)
                    for r_id in self.reps:
                        self.send(node, r_id, out_prep)

        # 3. in-prepare2 (Representative -> Group Nodes)
        elif msg.msg_type == MsgType.IN_PREPARE2:
            # Algorithm 1 verification
            is_consistent = (msg.content == st["proposal_value"])
            
            if is_consistent:
                self.log(f"[in-prepare2] Received valid confirmation from Rep. Watchdog satisfied.", source=f"node-{node.node_id}")
                st["intra_group_done"] = True
                st["state"] = ConsensusState.PRE_PREPARED
            else:
                self.log(f"[in-prepare2] ALARM! Inconsistent message from Rep {msg.sender_id}. Triggering emergency broadcast.", source=f"node-{node.node_id}")
                self._trigger_watchdog_broadcast(node, msg)

        # 4. out-prepare (Representative -> All Representatives)
        elif msg.msg_type == MsgType.OUT_PREPARE:
            if node.is_group_representative:
                sender_group = self.node_group_map[msg.sender_id]
                
                # If we get a message from a Representative, it represents the whole group (weight R)
                # If it's a watchdog broadcast from a member, it's weight 1.
                # "otherwise, the number of valid signatures is calculated as the number of votes."
                st["out_prepare_votes"][msg.sender_id] = msg.weight
                
                # Check Global Consensus
                current_weight = sum(st["out_prepare_votes"].values())
                required_weight = (2 * self.config.n) // 3
                
                if current_weight > required_weight and not st["inter_group_done"]:
                    st["inter_group_done"] = True
                    st["state"] = ConsensusState.PREPARED
                    self.log(f"[out-prepare] Global weighted quorum reached ({current_weight}). Sending commit to Global Primary.", source=f"node-{node.node_id}")
                    # commit: Representative -> Global Primary (Replica 0)
                    commit_msg = Message(MsgType.COMMIT, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content)
                    self.send(node, self.global_primary_id, commit_msg)

        # 5. commit (Representatives -> Global Primary)
        elif msg.msg_type == MsgType.COMMIT:
            if node.node_id == self.global_primary_id: # Aggregator
                st["commit_votes"].add(msg.sender_id)
                
                # Paper: Threshold analysis for inter-group consensus
                threshold = (len(self.reps) * 2) // 3
                
                if len(st["commit_votes"]) > threshold and not self.consensus_reached:
                    st["state"] = ConsensusState.COMMITTED
                    self.log(f"[commit] Global Primary aggregated signatures. Broadcasting preprepare2 to whole network.", source=f"node-{node.node_id}")
                    # preprepare2: Global Primary -> All nodes
                    prep2 = Message(MsgType.PREPREPARE2, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content)
                    for n in self.nodes:
                        self.send(node, n.node_id, prep2)

        # 6. preprepare2 (Replica 0 -> All Nodes)
        elif msg.msg_type == MsgType.PREPREPARE2:
            st["state"] = ConsensusState.DECIDED
            self.log(f"[preprepare2] Global consensus finalized. Sending REPLY to Client.", source=f"node-{node.node_id}")
            self.decided_value = msg.content
            self.consensus_reached = True
            self.consensus_event.set()
            # Reply: All nodes -> Client
            self.send(node, -1, Message(MsgType.REPLY, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content))

    async def _watchdog_timer(self, node: Node, original_msg: Message, rep_id: int):
        """Algorithm 1 Watchdog: Waits for the representative to broadcast the group decision."""
        # Simulated timeout based on protocol parameters
        await asyncio.sleep(0.5) 
        
        key = (original_msg.view, original_msg.sequence_number)
        st = self.node_states[node.node_id].get(key)
        
        # If the local group consensus hasn't finished, the representative is likely Byzantine/Silent
        if st and not st["intra_group_done"] and not st["watchdog_triggered"]:
            self.log(f"[WATCHDOG] Rep {rep_id} TIMEOUT. Peer-to-Network broadcast triggered.", source=f"node-{node.node_id}")
            self._trigger_watchdog_broadcast(node, original_msg)

    def _trigger_watchdog_broadcast(self, node: Node, original_msg: Message):
        """Bypasses a faulty representative to send the node's vote directly to the network."""
        key = (original_msg.view, original_msg.sequence_number)
        st = self.node_states[node.node_id][key]
        if st["watchdog_triggered"]: return
        st["watchdog_triggered"] = True
        
        # Consistent with Node Decision Broadcast Model: send individual vote (weight=1)
        out_prep = Message(MsgType.OUT_PREPARE, node.node_id, original_msg.view, original_msg.sequence_number, original_msg.digest, content=st["proposal_value"], weight=1)
        for r_id in self.reps:
            self.send(node, r_id, out_prep)

