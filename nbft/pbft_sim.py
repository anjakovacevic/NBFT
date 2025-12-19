import collections
from typing import List, Dict, Set, Optional
import asyncio
import time
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
        import random
        # Randomly select Byzantine nodes to allow the Primary (Node 0) to be faulty.
        # This highlights PBFT's vulnerability to leader failure (without view change).
        byz_indices = set(random.sample(range(self.config.n), self.config.actual_byzantine))
        
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

    def log(self, msg: str, source: str = "simulation", level: str = "INFO"):
        if not hasattr(self, 'verbose_logging'): self.verbose_logging = True
        formatted_msg = f"{level} - {source} - {msg}"
        self.logs.append(formatted_msg)

    def send_multicast(self, sender: Node, msg: Message):
        for target in self.nodes:
            asyncio.create_task(self._async_send(sender, target, msg))

    async def _async_send(self, sender: Node, target: Node, msg: Message):
        final_msg = ByzantineBehavior.apply_behavior(sender, msg, target.node_id)
        if final_msg is None: return
        
        # Latency model
        latency = 0.001
        await asyncio.sleep(latency)
        
        cloned_msg = Message(
            msg_type=final_msg.msg_type,
            sender_id=final_msg.sender_id,
            view=final_msg.view,
            sequence_number=final_msg.sequence_number,
            digest=final_msg.digest,
            content=final_msg.content
        )
        
        self.message_count += 1
        self.phase_counts[msg.msg_type.name] += 1
        
        self._handle_message(target, cloned_msg)

    async def run(self) -> RunResult:
        self.log("Starting PBFT simulation (Async)", source="simulation")
        self.consensus_event = asyncio.Event()
        self.start_time = time.time()
        
        primary_id = 0 
        req_msg = Message(MsgType.PBFT_PRE_PREPARE, primary_id, 0, 1, "digest_req", "VALUE_X")
        
        primary = self.nodes[primary_id]
        
        self.send_multicast(primary, req_msg)
        
        try:
            await asyncio.wait_for(self.consensus_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            self.log("Simulation timed out", source="simulation", level="ERROR")
            
        duration = time.time() - self.start_time
                
        return RunResult(
            success=self.consensus_reached,
            consensus_time=duration,
            total_messages=self.message_count,
            messages_per_phase=dict(self.phase_counts),
            decided_value=self.decided_value,
            logs=self.logs,
            message_trace=[],
            byzantine_nodes=[n.node_id for n in self.nodes if n.is_byzantine]
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
                "state": ConsensusState.IDLE,
                "proposal_value": None
            }
        
        st = self.node_states[node.node_id][state_key]
        
        # Logic Flow
        if msg.msg_type == MsgType.PBFT_PRE_PREPARE:
            if st["state"] == ConsensusState.IDLE:
                st["state"] = ConsensusState.PRE_PREPARED
                st["pre_prepare"] = True
                st["proposal_value"] = msg.content
                
                self.log(f"Received PrePrepare with value: {msg.content}", source=f"node-{node.node_id}")

                # Broadcast PREPARE
                reply = Message(MsgType.PBFT_PREPARE, node.node_id, msg.view, msg.sequence_number, msg.digest, content=msg.content)
                self.send_multicast(node, reply)
                
        elif msg.msg_type == MsgType.PBFT_PREPARE:
            st["prepare_votes"].add(msg.sender_id)
            if len(st["prepare_votes"]) >= quorum and st["state"] == ConsensusState.PRE_PREPARED:
                st["state"] = ConsensusState.PREPARED
                
                self.log(f"Prepared (Votes: {len(st['prepare_votes'])} >= Threshold: {quorum})", source=f"node-{node.node_id}")

                # Broadcast COMMIT
                commit = Message(MsgType.PBFT_COMMIT, node.node_id, msg.view, msg.sequence_number, msg.digest, content=st["proposal_value"])
                self.send_multicast(node, commit)
                
        elif msg.msg_type == MsgType.PBFT_COMMIT:
            st["commit_votes"].add(msg.sender_id)
            if len(st["commit_votes"]) >= quorum and st["state"] == ConsensusState.PREPARED:
                st["state"] = ConsensusState.COMMITTED
                # Execute/Decide
                # For simulation, if >= f+1 honest nodes decide, we consider it global success
                # if ANY honest node decides
                self.decided_value = st["proposal_value"] or "VALUE_X"
                
                if not node.is_byzantine:
                    if not self.consensus_reached:
                         self.log(f"Reached Consensus on: {self.decided_value}", source=f"node-{node.node_id}")
                         # Log global success once
                         self.log(f"[GLOBAL] Consensus reached: value='{self.decided_value}'", source="simulation")
                    
                    self.consensus_reached = True
                    self.consensus_event.set()
