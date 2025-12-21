import hashlib
from typing import List, Tuple, Dict, Optional
from .models import Node, Group

def sha32(data: bytes) -> int:
    """First 32 bits of SHA-256 as an integer."""
    return int.from_bytes(hashlib.sha256(data).digest()[:4], "big")

RING_SIZE = 2**32

class ConsistentHashing:
    """
    Matches the paper, but replaces:
      - nodeip with a stable string id (e.g. f"node_{node_id}")
      - masterip with the previous primary's stable string id

    Logic:
      pos(node) = hash(nodeip)
      primary = clockwise nearest to hash(masterip + previoushash + viewnumber)
      start = round(viewnumber / nodenumber)
      walk clockwise, skip primary, every m nodes -> one group
      rep(group) = clockwise nearest (within group) to hash(masterip + viewnumber + groupnumber)
      R = floor((n-1)/m)
    """

    def __init__(self, nodes: List[Node], group_size_m: int):
        if group_size_m <= 0:
            raise ValueError("group_size_m must be > 0")
        self.nodes = nodes
        self.m = group_size_m              # paper: nodes per group
        self.n = len(nodes)

        # Stable id string that replaces "nodeip"
        self.node_key: Dict[int, str] = {nd.node_id: f"node_{nd.node_id}" for nd in nodes}

        # Precompute ring positions
        self.pos_by_id: Dict[int, int] = {
            nd.node_id: sha32(self.node_key[nd.node_id].encode())
            for nd in nodes
        }

        # Clockwise order
        self.clockwise_nodes: List[Node] = sorted(nodes, key=lambda nd: self.pos_by_id[nd.node_id])

    def get_R(self) -> int:
        """R = ceil((n-1)/m), groups contain all non-primary nodes."""
        if self.n <= 1:
            return 0
        return ((self.n - 1) + self.m - 1) // self.m  # integer ceil

    def _clockwise_nearest(self, target_pos: int, candidates: List[Node]) -> int:
        """Return candidate node_id whose ring pos is nearest clockwise from target_pos."""
        ordered = sorted(candidates, key=lambda nd: self.pos_by_id[nd.node_id])
        for nd in ordered:
            if self.pos_by_id[nd.node_id] >= target_pos:
                return nd.node_id
        return ordered[0].node_id  # wrap-around

    def get_global_primary(
        self,
        view_number: int,
        previoushash: str = "0"*64,
        prev_primary_id: Optional[int] = None,
    ) -> int:
        """
        Primary is clockwise nearest to hash(masterip + previoushash + viewnumber),
        where masterip is the previous round primary "ip".
        Simulate masterip with self.node_key[prev_primary_id]

        If prev_primary_id is None (first round), pick the first node clockwise as primary
        """
        if prev_primary_id is None:
            return self.clockwise_nodes[0].node_id

        masterip_sim = self.node_key[prev_primary_id]  # replaces "masterip"
        h_in = f"{masterip_sim}{previoushash}{view_number}".encode()
        target = sha32(h_in)
        return self._clockwise_nearest(target, self.clockwise_nodes)

    def form_groups(
        self,
        view_number: int,
        previoushash: str = "0"*64,
        prev_primary_id: Optional[int] = None,
    ) -> Tuple[List[Group], Dict[int, int], int]:
        """
        Returns (groups, node_group_map, global_primary_id)

        Grouping logic:
          start index = round(viewnumber/nodenumber)
          walk clockwise, skip primary, every m nodes -> group
        """
        primary_id = self.get_global_primary(view_number, previoushash, prev_primary_id)

        # Start from [viewnumber/nodenumber] (rounded)
        start_idx = int(round(view_number / max(self.n, 1))) % self.n

        ordered = [self.clockwise_nodes[(start_idx + i) % self.n] for i in range(self.n)]
        non_primary = [nd for nd in ordered if nd.node_id != primary_id]  # skip primary

        R = self.get_R()
        groups: List[Group] = []
        node_group_map: Dict[int, int] = {primary_id: -1}  # primary excluded from groups

        # Contiguous chunks of size m. Ensure ALL non-primary nodes are assigned.
        for g_id in range(R):
            # If it's the last group, take all remaining nodes
            #if g_id == R - 1:
            #    chunk = non_primary[g_id * self.m :]
            #else:
            #    chunk = non_primary[g_id * self.m : (g_id + 1) * self.m]
            chunk = non_primary[g_id * self.m : (g_id + 1) * self.m]  # <= m members always
            if not chunk:
                break  # safety, shouldn't happen with correct R
            member_ids = [nd.node_id for nd in chunk]
            for nid in member_ids:
                node_group_map[nid] = g_id

            # Representative: hash(masterip + viewnumber + groupnumber)
            if prev_primary_id is None:
                masterip_sim = self.node_key[primary_id]
            else:
                masterip_sim = self.node_key[prev_primary_id]

            rep_target = sha32(f"{masterip_sim}{view_number}{g_id}".encode())
            rep_id = self._clockwise_nearest(rep_target, chunk) if chunk else -1

            groups.append(Group(
                group_id=g_id,
                representative_id=rep_id,
                members=member_ids
            ))

        return groups, node_group_map, primary_id
