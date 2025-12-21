import hashlib
import struct
from typing import List, Tuple, Dict
from .models import Node, Group

class ConsistentHashing:
    """
    Implements the Consistent Hashing and Grouping logic
    
    - The ID space is a ring [0, 2^32 - 1].
    - Nodes are mapped to points on the ring.
    - Groups are formed by partitioning the ring or the sorted node list.
    """

    def __init__(self, nodes: List[Node], m_groups: int):
        self.nodes = nodes
        self.m = m_groups
        self.ring_max = 2**32
        
        # Sort nodes by ID for deterministic grouping
        self.sorted_nodes = sorted(nodes, key=lambda n: n.node_id)
        self.n = len(nodes)

    def get_global_primary(self, view_number: int) -> int:
        """
        hash(masterip + viewnumber) -> Clockwise nearest node.
        Track 'masterip' as the primary of the previous consensus round (or node 0)
        """
        # For simulation, we simplify 'masterip' to the primary of view v-1
        # or just node 0 for the first view.
        if view_number == 0:
            prev_primary = 0
        else:
            prev_primary = (view_number - 1) % self.n
            
        h_input = f"{prev_primary}_{view_number}".encode()
        target_pos = int.from_bytes(hashlib.sha256(h_input).digest()[:4], "big")
        
        # Consistent Hashing: find clockwise nearest node
        node_positions = []
        for node in self.nodes:
            h = hashlib.sha256(f"node_{node.node_id}".encode()).digest()
            pos = int.from_bytes(h[:4], "big")
            node_positions.append((pos, node.node_id))
        
        node_positions.sort()
        
        for pos, n_id in node_positions:
            if pos >= target_pos:
                return n_id
        return node_positions[0][1] # Wrap around

    def form_groups(self, view_number: int) -> Tuple[List[Group], Dict[int, int]]:
        """
        Partitions nodes using the stride/skip logic:
        1. Sort nodes clockwise on ring.
        2. Identify Global Primary.
        3. Starting from [view/n], skip primary, assign nodes round-robin (every m).
        """
        # 1. Map and Sort Nodes
        node_positions = []
        for node in self.nodes:
            h = hashlib.sha256(f"node_{node.node_id}".encode()).digest()
            pos = int.from_bytes(h[:4], "big")
            node_positions.append((pos, node))
        node_positions.sort()
        
        primary_id = self.get_global_primary(view_number)
        
        # 2. Extract nodes in order, starting from offset [view/n]
        start_idx = view_number % self.n
        ordered_nodes = [node_positions[(start_idx + i) % self.n][1] for i in range(self.n)]
        
        # 3. Filter out Primary and assign to m groups
        group_buckets = {g_id: [] for g_id in range(self.m)}
        node_group_map = {}
        
        non_primary_nodes = [n for n in ordered_nodes if n.node_id != primary_id]
        
        for i, node in enumerate(non_primary_nodes):
            g_id = i % self.m # "Every m nodes to a consensus group" logic
            group_buckets[g_id].append(node)
            node_group_map[node.node_id] = g_id
        
        # Primary also needs a group for intra-group protocol consistency 
        node_group_map[primary_id] = 0 # Default primary to group 0
        if primary_id not in [n.node_id for n in group_buckets[0]]:
            primary_node = next(n for n in self.nodes if n.node_id == primary_id)
            group_buckets[0].append(primary_node)

        # 4. Select Representatives based on hash(primary + view + group_id)
        groups = []
        for g_id in range(self.m):
            members = group_buckets[g_id]
            member_ids = [n.node_id for n in members]
            
            # Select Representative
            rep_h_input = f"{primary_id}_{view_number}_{g_id}".encode()
            rep_target = int.from_bytes(hashlib.sha256(rep_h_input).digest()[:4], "big")
            
            best_rep_id = member_ids[0]
            min_dist = 2**32
            
            for node in members:
                # Get node's ring pos
                h = hashlib.sha256(f"node_{node.node_id}".encode()).digest()
                pos = int.from_bytes(h[:4], "big")
                dist = (pos - rep_target) % (2**32)
                if dist < min_dist:
                    min_dist = dist
                    best_rep_id = node.node_id
            
            groups.append(Group(
                group_id=g_id,
                representative_id=best_rep_id,
                members=member_ids
            ))
            
        return groups, node_group_map

    def get_R(self) -> int:
        """Paper formula R = floor((n-1)/m)"""
        return (self.n - 1) // self.m if self.m > 0 else 0
