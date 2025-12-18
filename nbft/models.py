from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Dict, Any, Set
import time
import hashlib

#  Enums for State Management 

class NodeType(Enum):
    HONEST = auto()
    BYZANTINE = auto()

class MsgType(Enum):
    # Common Types
    REQUEST = auto()
    
    # Standard PBFT Types (Used by PBFT simulator)
    PBFT_PRE_PREPARE = auto()
    PBFT_PREPARE = auto()
    PBFT_COMMIT = auto()
    
    # NBFT Specific Types (Paper Terminology)
    # Intra-group phases
    PREPREPARE1 = auto() # Global Primary -> Representatives
    IN_PREPARE1 = auto() # Representative -> Group Members
    IN_PREPARE2 = auto() # Group Members -> Representative
    LOCAL_COMMIT = auto() # Representative -> Members (Result confirmation / Watchdog)
    ALARM = auto() # Member -> Network (Fraud detection)
    
    # Inter-group phases
    OUT_PREPARE = auto() # Representative -> All Representatives (Weighted broadcast)
    COMMIT = auto() # Among Representatives (Final agreement)
    PREPREPARE2 = auto() # Global Success -> All Members (Final dissemination)
    
    # Standard Finalization & View Change
    REPLY = auto() # To Client
    VIEW_CHANGE = auto() # Broadcast when timer expires
    NEW_VIEW = auto() # New Primary announces new view

class ConsensusState(Enum):
    IDLE = auto()
    PRE_PREPARED = auto()
    PREPARED = auto()
    COMMITTED = auto()
    DECIDED = auto() # NBFT specific

#  Core Data Models 

@dataclass
class Node:
    """
    Represents a single participant in the network.
    Maps to the basic system model where nodes can be honest or byzantine.
    """
    node_id: int
    public_key: str # Placeholder for PKI
    is_byzantine: bool = False
    byzantine_strategy: str = "none" # e.g. "silent", "equivocator"
    
    # NBFT specific grouping fields
    group_id: int = -1
    is_global_primary: bool = False
    is_group_representative: bool = False

    def __hash__(self):
        return self.node_id

@dataclass
class Message:
    """
    A network message
    """
    msg_type: MsgType
    sender_id: int
    view: int
    sequence_number: int
    digest: str # Hash of the request/content
    content: Any = None # Payload (e.g., client request or simplified value 'v')
    signature: Optional[str] = None # Digital Signature
    weight: int = 1 # NBFT weighted voting: number of valid signatures collected
    
    # Simulation metadata (not part of protocol, strictly for analysis)
    timestamp: float = field(default_factory=time.time)
    
    def __repr__(self):
        return f"<Msg {self.msg_type.name} from {self.sender_id} view={self.view} seq={self.sequence_number}>"
        
    def __lt__(self, other):
        # Tie-breaker for priority queue: just compare timestamps or use arbitrary ID comparison
        # Since timestamps are the primary sort key in the queue tuple, this is only called if timestamps match.
        return self.timestamp < other.timestamp
        
    @staticmethod
    def sign(content: str, node_id: int) -> str:
        """Simulates signing content with node's private key."""
        # Simple simulated signature: H(content + node_secret)
        return hashlib.sha256(f"{content}_SECRET_{node_id}".encode()).hexdigest()

    @staticmethod
    def verify(content: str, signature: str, node_id: int) -> bool:
        """Verifies signature matches the expected public key owner."""
        expected = hashlib.sha256(f"{content}_SECRET_{node_id}".encode()).hexdigest()
        return signature == expected

@dataclass
class Vote:
    """
    NBFT specific: A condensed vote sent from group members to representatives.
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
    Nodes are divided into m groups.
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
