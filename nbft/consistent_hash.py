import hashlib
import struct
from typing import List, Tuple, Dict
from .models import Node, Group

class ConsistentHashing:
    """
    Implements the Consistent Hashing and Grouping logic
    
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
        Partitions nodes into m groups using Consistent Hashing (Ring Topology).
        """
        groups = []
        node_group_map = {}
        
        # 1. Define Ring Space
        RING_SIZE = 2**32
        
        # 2. Place "Virtual Points" for Groups on the Ring
        # We shift these points based on view_number to ensure rotation of members
        # Hash(view_number, group_id) -> position
        group_points = []
        for g_id in range(self.m):
            h = hashlib.sha256(f"view_{view_number}_group_{g_id}".encode()).digest()
            point = int.from_bytes(h[:4], "big") # Take first 4 bytes for 32-bit int
            group_points.append((point, g_id))
        
        # Sort group points by position on ring
        group_points.sort()
        
        # 3. Initialize buckets for groups
        group_buckets = {g_id: [] for _, g_id in group_points}
        
        # 4. Map Nodes to Ring
        # Node position = Hash(node_id)
        # We can also add view_number to salt if we want nodes to move "logically", 
        # but usually nodes are fixed and buckets move.
        node_positions = []
        for node in self.nodes:
            # We use node_id as stable identity
            h = hashlib.sha256(f"node_{node.node_id}".encode()).digest()
            pos = int.from_bytes(h[:4], "big")
            node_positions.append((pos, node))
            
            # 5. Assign Nodes to Groups (Generic Consistent Hashing assignment)
            # Find the first group point >= node_point (clockwise)
            # If none found, wrap around to index 0.
                
            # 6. Bounded Load Balancing
            # Standard CH can result in severe imbalance for small N (e.g., 8 vs 2).
            # We enforce a soft cap: ceil(N/M) + 1
            max_capacity = (self.n + self.m - 1) // self.m + 1 # slightly flexible cap
            
            # Note: The simple greedy assignment above might overfill buckets.
            # We need to re-distribute? Or use iterative assignment.
            
            # Better approach for Bounded Load:
            # - Sort nodes by ring position
            # - Iterate nodes and find nearest AVAILABLE bucket
            
        # Re-doing assignment with Capacity Check
        groups_capacity = {gid: 0 for gid in range(self.m)}
        group_buckets = {gid: [] for gid in range(self.m)} # Reset buckets
        max_capacity = (self.n // self.m) + 1 # Strict balance: N/M + 1
            
        # Sort nodes by position
        node_positions.sort() # Sort by pos
            
        for n_pos, node in node_positions:
            # Find best group for this node
            # We iterate through group_points (sorted) to find the first one that fits
            # The 'start_index' corresponds to standard CH mapping
            
            # Find index in group_points where g_pos >= n_pos
            start_idx = 0
            for i, (g_pos, _) in enumerate(group_points):
                if g_pos >= n_pos:
                    start_idx = i
                    break
            
            # Strategy: Search cyclically starting from 'start_idx' for a non-full group
            assigned = False
            for i in range(self.m):
                idx = (start_idx + i) % self.m
                _, g_id = group_points[idx]
                
                if groups_capacity[g_id] < max_capacity:
                    groups_capacity[g_id] += 1
                    group_buckets[g_id].append((n_pos, node))
                    node_group_map[node.node_id] = g_id
                    assigned = True
                    break
            
            # Fallback: if all full (should be rare/impossible if cap is correct), force into start_idx
            if not assigned:
                 _, g_id = group_points[start_idx % self.m]
                 group_buckets[g_id].append((n_pos, node))
                 node_group_map[node.node_id] = g_id
        
        # 7. Create Group Objects and Select Representatives
        for g_id in range(self.m):
            members_with_pos = group_buckets[g_id]
            member_ids = [n.node_id for _, n in members_with_pos]
            
            if not member_ids:
                # Edge case: Empty group (possible with small N and bad luck)
                # Fallback: Steal a node? Or just handle empty groups upstream?
                # GUI handles N >> M 
                rep_id = -1
            else:
                # SELECT REPRESENTATIVE
                # Theory: Node closest to the "Group Point" (hashed ID) is the Rep.
                # In our assignment, the first node in the bucket (if sorted) is naturally closest 
                # to the *previous* boundary, or the one closest to the group point.
                # Let's say: The node with the smallest distance to the Group Point.
                
                # Find simulated group point again
                g_pos = next(p for p, gid in group_points if gid == g_id)
                
                # Sort members by distance to g_pos on the ring (counter-clockwise distance)
                # distance = (g_pos - n_pos) % RING_SIZE
                # The node that "falls into" the bucket most recently is closest.
                
                best_rep = None
                min_dist = float('inf')
                
                for n_pos, node in members_with_pos:
                    # Distance in ring arithmetic
                    dist = (g_pos - n_pos) % RING_SIZE
                    if dist < min_dist:
                        min_dist = dist
                        best_rep = node
                
                rep_id = best_rep.node_id

            group = Group(
                group_id=g_id,
                representative_id=rep_id,
                members=member_ids
            )
            groups.append(group)
            
        return groups, node_group_map

    def get_R(self) -> int:
        """Paper formula R = floor((n-1)/m)"""
        # Note: The formula assumes the primary is excluded from the groups or handled separately?
        # Actually, standard PBFT n=3f+1. NBFT uses m groups.
        # If we just partition n nodes, R = n/m.
        # The paper formula `R = floor((n-1)/m)` specifically likely excludes the global primary.
        # We will follow the paper formula for the Analysis class, but use real sizes for Simulation.
        return (self.n - 1) // self.m if self.m > 0 else 0
