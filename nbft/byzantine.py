from typing import List, Optional
import random
from .models import Node, Message, MsgType, Vote

class ByzantineBehavior:
    """
    Implements the Byzantine failure modes described in Section 4.1.
    """
    
    @staticmethod
    def apply_behavior(node: Node, message: Message, target_id: int) -> Optional[Message]:
        """
        Intercepts an outgoing message and potentially modifies/drops it based on strategy.
        Returns:
            - The original message (honest)
            - Modified message (malicious)
            - None (silent/drop)
        """
        if not node.is_byzantine:
            return message
            
        strategy = node.byzantine_strategy.lower()
        
        if strategy == "silent":
            # Section 4.1: "Silent failure" - node stops sending messages
            return None
            
        elif strategy == "equivocation":
            # Sends conflicting information to different nodes.
            # Simplified: Flip a bit in the digest for half the targets
            if target_id % 2 == 0:
                message.digest = message.digest[::-1] # Reverse hash to corrupt
                message.signature = "INVALID"
            return message
            
        elif strategy == "bad_aggregator":
            # Relevant for NBFT Representatives. 
            # They might claim they have enough votes when they don't,
            # or forward a decision they didn't actually reach.
            if node.is_group_representative and message.msg_type == MsgType.REP_PREPARE:
                 message.digest = "CORRUPTED_AGGREGATE"
            return message

        elif strategy == "random_noise":
             # Just sends garbage data
             message.content = "GARBAGE"
             return message
             
        # Fallback: act honest if strategy unknown
        return message

    @staticmethod
    def corrupt_vote(node: Node, vote: Vote) -> Optional[Vote]:
        if not node.is_byzantine:
            return vote
            
        strategy = node.byzantine_strategy.lower()
        
        if strategy == "silent":
            return None
            
        if strategy in ["equivocation", "targeted_sabotage"]:
            vote.digest = "FALSE_DIGEST"
        
        return vote
