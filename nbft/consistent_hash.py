import hashlib
import struct
from typing import List, Tuple, Dict
from .models import Node, Group

class ConsistentHashing:
    """
    Implements the Consistent Hashing and Grouping logic described in Section 3.1
    of the simulator paper.
    
    The paper simplifies standard consistent hashing:
    - The ID space is a ring [0, 2^32 - 1].
    - Nodes are mapped to points on the ring.
    - Groups are formed by partitioning the ring or the sorted node list.
    """

    def __init__(self, nodes: List[Node], m_groups: int):
        self.nodes = nodes
        self.m = m_groups
        self.ring_max = 2**32
        
        # Sort nodes by ID for deterministic grouping (simplified from real hashing for educational clarity)
        # In a real impl, we would use hash(node.public_key), but the paper implies 
        # a logical structure for easy reproduction.
        self.sorted_nodes = sorted(nodes, key=lambda n: n.node_id)
        self.n = len(nodes)

    def get_global_primary(self, view_number: int) -> int:
        """
        Determines the global primary based on view number.
        Paper implies standard PBFT rotation: p = v mod n
        """
        if not self.sorted_nodes:
            return -1
        primary_index = view_number % self.n
        return self.sorted_nodes[primary_index].node_id

    def form_groups(self, view_number: int) -> Tuple[List[Group], Dict[int, int]]:
        """
        Partitions nodes into m groups.
        """
        groups = []
        node_group_map = {}
        
        import random
        # Create a view-specific shuffling of nodes
        # This ensures Representatives rotate every view.
        # We seed with view_number for determinism across all nodes.
        view_nodes = list(self.sorted_nodes)
        
        # Use a localized Random instance to avoid affecting global random state
        rng = random.Random(view_number)
        rng.shuffle(view_nodes)
        
        base_size = self.n // self.m
        remainder = self.n % self.m
        
        # Identify global primary
        global_primary_id = self.get_global_primary(view_number)
        
        current_idx = 0
        for i in range(self.m):
            size = base_size + (1 if i < remainder else 0)
            group_members_indices = range(current_idx, current_idx + size)
            
            member_ids = [view_nodes[idx].node_id for idx in group_members_indices]
            
            # Select Representative:
            # The paper says each group has a representative.
            # Strategy: The node with the highest ID in the group, 
            # or the one closest to some hash.
            # Simple Strategy: First member is representative.
            rep_id = member_ids[0]
            
            # Check if global primary is here, maybe force them to be rep?
            # Paper doesn't strictly specify election inside group for rep, 
            # often it's deterministic.
            if global_primary_id in member_ids:
                # If global primary is in this group, they dictate the flow.
                pass 

            group = Group(
                group_id=i,
                representative_id=rep_id,
                members=member_ids
            )
            groups.append(group)
            
            for ptr in member_ids:
                node_group_map[ptr] = i
            
            current_idx += size
            
        return groups, node_group_map

    def get_R(self) -> int:
        """Paper formula R = floor((n-1)/m)"""
        # Note: The formula assumes the primary is excluded from the groups or handled separately?
        # Actually, standard PBFT n=3f+1. NBFT uses m groups.
        # If we just partition n nodes, R = n/m.
        # The paper formula `R = floor((n-1)/m)` specifically likely excludes the global primary.
        # We will follow the paper formula for the Analysis class, but use real sizes for Simulation.
        return (self.n - 1) // self.m if self.m > 0 else 0
