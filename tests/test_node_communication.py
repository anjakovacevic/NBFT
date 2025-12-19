import unittest
from nbft.models import Node, Message, MsgType, Vote
from nbft.byzantine import ByzantineBehavior

class TestNodeCommunication(unittest.TestCase):
    def setUp(self):
        # Create some nodes
        self.node_honest = Node(node_id=0, public_key="pub_0", is_byzantine=False)
        self.node_silent = Node(node_id=1, public_key="pub_1", is_byzantine=True, byzantine_strategy="silent")
        self.node_equivocator = Node(node_id=2, public_key="pub_2", is_byzantine=True, byzantine_strategy="equivocation")
        self.node_bad_rep = Node(node_id=3, public_key="pub_3", is_byzantine=True, byzantine_strategy="bad_aggregator", is_group_representative=True)
        
        # A common message
        self.msg = Message(
            msg_type=MsgType.PREPREPARE1,
            sender_id=0,
            view=0,
            sequence_number=1,
            digest="original_digest",
            content="original_content"
        )

    def test_honest_message_untouched(self):
        """Test that an honest node's message is not modified."""
        print("\n Testing Honest Node Behavior ")
        final_msg = ByzantineBehavior.apply_behavior(self.node_honest, self.msg, target_id=10)
        print(f"Original digest: {self.msg.digest}")
        print(f"Resulting digest: {final_msg.digest}")
        self.assertIsNotNone(final_msg)
        self.assertEqual(final_msg.digest, "original_digest")

    def test_byzantine_silent_failure(self):
        """Test that a silent byzantine node drops messages (returns None)."""
        print("\n Testing Byzantine Silent Failure ")
        final_msg = ByzantineBehavior.apply_behavior(self.node_silent, self.msg, target_id=10)
        print(f"Message sent by Node {self.node_silent.node_id} (Silent Strategy)")
        print(f"Result: {'Message Dropped' if final_msg is None else 'Message Sent'}")
        self.assertIsNone(final_msg)

    def test_byzantine_equivocation(self):
        """Test that an equivocator sends different/corrupted messages to different nodes."""
        print("\n Testing Byzantine Equivocation (Double-Talk) ")
        msg_template = Message(MsgType.PREPREPARE1, 2, 0, 1, "ORIGINAL")
        
        # Target ID even -> Corrupted
        final_even = ByzantineBehavior.apply_behavior(self.node_equivocator, msg_template, target_id=0)
        print(f"Sent to Node 0 (even): {final_even.digest}")
        
        # Target ID odd -> Untouched
        final_odd = ByzantineBehavior.apply_behavior(self.node_equivocator, msg_template, target_id=1)
        print(f"Sent to Node 1 (odd):  {final_odd.digest}")

        self.assertTrue(final_even.digest.startswith("CORRUPTED_"))
        self.assertEqual(final_odd.digest, "ORIGINAL")

    def test_byzantine_bad_aggregator_behavior(self):
        """Test that a bad aggregator modifies specific message types."""
        print("\n Testing Bad Aggregator (Representative Sabotage) ")
        msg_out = Message(MsgType.OUT_PREPARE, 3, 0, 1, "honest_digest", content="honest_value")
        final_msg = ByzantineBehavior.apply_behavior(self.node_bad_rep, msg_out, target_id=5)
        
        print(f"Representative sent {msg_out.msg_type.name}")
        print(f"Malicious Digest: {final_msg.digest}")
        print(f"Malicious Content: {final_msg.content}")

        self.assertEqual(final_msg.digest, "CORRUPTED_AGGREGATE")
        self.assertEqual(final_msg.content, "MALICIOUS_VALUE")

    def test_signature_verification(self):
        """Test that honest nodes can verify each other, but signature fails if digest is tampered."""
        print("\n Testing Cryptographic Signature Verification ")
        content = "consensus_data"
        signature = Message.sign(content, self.node_honest.node_id)
        print(f"Node {self.node_honest.node_id} signed content: '{content}'")
        
        # Verify valid
        valid = Message.verify(content, signature, self.node_honest.node_id)
        print(f"Verification with correct data: {'PASSED' if valid else 'FAILED'}")
        self.assertTrue(valid)

        # Verify tampered
        tampered_content = "malicious_data"
        invalid = Message.verify(tampered_content, signature, self.node_honest.node_id)
        print(f"Verification with tampered data: {'PASSED' if invalid else 'FAILED'}")
        self.assertFalse(invalid)

    def test_byzantine_corrupt_vote(self):
        """Test that corrupt_vote modifies the vote digest for byzantine nodes."""
        print("\n Testing Byzantine Vote Corruption ")
        vote = Vote(voter_id=2, view=0, digest="GOOD_DIGEST", signature="SIG")
        
        bad_vote = ByzantineBehavior.corrupt_vote(self.node_equivocator, vote)
        print(f"Original Vote Digest: {vote.digest}")
        print(f"Byzantine Vote Digest: {bad_vote.digest}")
        self.assertEqual(bad_vote.digest, "FALSE_DIGEST")

    def test_fraud_detection_logic(self):
        """Shows how a group member detects fraud from its representative."""
        print("\n Testing Fraud Detection Logic (Member Perspective) ")
        expected_value = "VALUE_A"
        msg_from_rep = Message(MsgType.IN_PREPARE2, 3, 0, 1, "BAD", content="MALICIOUS_VALUE")
        
        print(f"Member expected: {expected_value}")
        print(f"Member received: {msg_from_rep.content}")
        
        is_consistent = (msg_from_rep.content == expected_value)
        print(f"Inconsistency detected? {'YES' if not is_consistent else 'NO'}")
        self.assertFalse(is_consistent)
        
    def test_equivocation_detected_by_receivers(self):
        """Shows how equivocation results in different digests for different receivers."""
        print("\n Testing Equivocation Visibility ")
        msg_template = Message(MsgType.PREPREPARE1, 2, 0, 1, "ORIGINAL")
        msg0 = ByzantineBehavior.apply_behavior(self.node_equivocator, msg_template, target_id=0)
        msg1 = ByzantineBehavior.apply_behavior(self.node_equivocator, msg_template, target_id=1)
        
        print(f"Receiver A (Node 0) got: {msg0.digest}")
        print(f"Receiver B (Node 1) got: {msg1.digest}")
        self.assertNotEqual(msg0.digest, msg1.digest)

if __name__ == "__main__":
    unittest.main()
