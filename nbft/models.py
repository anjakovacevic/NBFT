from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Dict, Any, Set
import time
import hashlib

# --- Enums for State Management ---

class NodeType(Enum):
    HONEST = auto()
    BYZANTINE = auto()

class MsgType(Enum):
    # PBFT Types
    PRE_PREPARE = auto()
    PREPARE = auto()
    COMMIT = auto()
    
    # NBFT Specific Types
    # Intra-group phases
    GROUP_PRE_PREPARE = auto() # Primary -> Group
    GROUP_VOTE = auto() # Group Members -> Representative
    GROUP_RESULT = auto() # Rep -> Group Members know the decision
    ALARM = auto() # Member -> Network (if the representative is not following the protocol)
    
    # Inter-group phases
    REP_PRE_PREPARE = auto() # Global Primary -> Representatives
    REP_PREPARE = auto() # Among Representatives
    REP_COMMIT = auto() # Among Representatives
    
    # View Change
    VIEW_CHANGE = auto() # Broadcast when timer expires
    NEW_VIEW = auto() # New Primary announces new view
    
    # Finalization
    REPLY = auto() # To Client
    FINAL_DECISION = auto() # Rep -> Group Members

class ConsensusState(Enum):
    IDLE = auto()
    PRE_PREPARED = auto()
    PREPARED = auto()
    COMMITTED = auto()
    DECIDED = auto() # NBFT specific

# --- Core Data Models ---

@dataclass
class Node:
    """
    Represents a single participant in the network.
    Maps to the basic system model where nodes can be honest or byzantine.
    """
    node_id: int
    public_key: str # Placeholder for PKI
    is_byzantine: bool = False
    byzantine_strategy: str = "none" # e.g., "silent", "equivocator"
    
    # NBFT specific grouping fields
    group_id: int = -1
    is_global_primary: bool = False
    is_group_representative: bool = False

    def __hash__(self):
        return self.node_id

@dataclass
class Message:
    """
    A network message.
    Simulates the packets exchanged in Section 2 and 3.
    """
    msg_type: MsgType
    sender_id: int
    view: int
    sequence_number: int
    digest: str # Hash of the request/content
    content: Any = None # Payload (e.g., client request or simplified value 'v')
    
    # Simulation metadata (not part of protocol, strictly for analysis)
    timestamp: float = field(default_factory=time.time)
    
    def __repr__(self):
        return f"<Msg {self.msg_type.name} from {self.sender_id} view={self.view} seq={self.sequence_number}>"
        
    def __lt__(self, other):
        # Tie-breaker for priority queue: just compare timestamps or use arbitrary ID comparison
        # Since timestamps are the primary sort key in the queue tuple, this is only called if timestamps match.
        return self.timestamp < other.timestamp

@dataclass
class Vote:
    """
    NBFT specific: A condensed vote sent from group members to representatives.
    Maps to Algorithm 2 logic where representatives count votes.
    """
    voter_id: int
    view: int
    digest: str
    signature: str # Placeholder for digital signature
    weight: int = 1 # NBFT might use reputation weights in extensions

@dataclass
class Group:
    """
    NBFT specific: A cluster of nodes.
    Maps to Section 3.1: nodes are divided into m groups.
    """
    group_id: int
    representative_id: int # The node acting as primary for this group
    members: List[int] # List of Node IDs
    
    @property
    def size(self):
        return len(self.members)

@dataclass
class RunConfig:
    """
    Configuration for a single simulation run.
    """
    algorithm: str # 'PBFT' or 'NBFT'
    n: int # Total nodes
    m: int # Number of groups (NBFT only)
    f: int = 0 # Max Byzantine nodes tolerance (theoretical)
    actual_byzantine: int = 0 # Actual number of bad nodes injected
    client_requests: int = 1
    latency_profile: str = "constant" # "random", "geo-distributed"

@dataclass
class RunResult:
    """
    Results for analysis.
    """
    success: bool
    consensus_time: float
    total_messages: int
    messages_per_phase: Dict[str, int]
    decided_value: Any
    logs: List[str]
    message_trace: List[Dict[str, Any]] = field(default_factory=list)
    byzantine_nodes: List[int] = field(default_factory=list)
