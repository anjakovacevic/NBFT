import unittest
import asyncio
from nbft.models import Node, RunConfig, MsgType
from nbft.consistent_hash import ConsistentHashing
from nbft.analysis import Analysis
from nbft.nbft_sim import NBFTSimulator

class TestNBFTProtocol(unittest.TestCase):
    def setUp(self):
        # Setup a basic 10-node network with 3 groups for testing
        self.n = 10
        self.m = 3
        self.nodes = [Node(node_id=i, public_key=f"pub_{i}") for i in range(self.n)]
        self.ch = ConsistentHashing(self.nodes, self.m)
        self.config = RunConfig(
            algorithm="NBFT",
            n=self.n,
            m=self.m,
            f=3,
            actual_byzantine=0,
            client_requests=1
        )

    def test_consistent_hashing_determinism(self):
        """Verify that group formation is deterministic for a given view."""
        print("\n Testing Consistent Hashing Determinism ")
        groups1, map1 = self.ch.form_groups(view_number=0)
        groups2, map2 = self.ch.form_groups(view_number=0)
        
        # Check if same view gives same results
        self.assertEqual(len(groups1), self.m)
        self.assertEqual(map1, map2)
        
        for i in range(self.m):
            self.assertEqual(groups1[i].representative_id, groups2[i].representative_id)
            self.assertEqual(sorted(groups1[i].members), sorted(groups2[i].members))
        
        print(f"Verified: View 0 always results in the same {self.m} groups.")

    def test_view_change_rotation(self):
        """Verify that the primary and group formation change with the view."""
        print("\n Testing View Change Rotation ")
        primary0 = self.ch.get_global_primary(view_number=0)
        primary1 = self.ch.get_global_primary(view_number=1)
        
        print(f"Primary View 0: {primary0}")
        print(f"Primary View 1: {primary1}")
        
        # In NBFT, primary should rotate or change to ensure fairness
        self.assertNotEqual(primary0, primary1, "Primary should change between views (statistically likely)")

    def test_mathematical_thresholds(self):
        """Verify paper formulas for R, w, and E."""
        print("\n Testing NBFT Mathematical Thresholds ")
        # For n=10, m=3:
        # R = floor((10-1)/3) = 3
        # w = floor((3-1)/3) = 0 (Group can only tolerate 0 failures if R=3)
        # E = floor((3-1)/3) = 0
        
        n, m = 10, 3
        R = Analysis.calculate_R(n, m)
        w = Analysis.calculate_w(R)
        E = Analysis.calculate_E(m)
        
        print(f"n={n}, m={m} -> R={R}, w={w}, E={E}")
        
        self.assertEqual(R, 3)
        self.assertEqual(w, 0)
        self.assertEqual(E, 0)
        
        # For larger scale: n=100, m=10
        # R = floor(99/10) = 9
        # w = floor(8/3) = 2
        R_large = Analysis.calculate_R(100, 10)
        w_large = Analysis.calculate_w(R_large)
        print(f"n=100, m=10 -> R={R_large}, w={w_large}")
        self.assertEqual(w_large, 2)

    def test_simulation_honest_consensus(self):
        """Run a full simulation with honest nodes to verify the end-to-end path."""
        print("\n Testing End-to-End Simulation (Honest) ")
        
        async def run_sim():
            sim = NBFTSimulator(self.config)
            # Run simulation
            result = await sim.run()
            return result

        # Run async code in sync test
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(run_sim())
        loop.close()

        print(f"Simulation Success: {result.success}")
        print(f"Decided Value: {result.decided_value}")
        print(f"Total Messages: {result.total_messages}")

        self.assertTrue(result.success, "Simulation should succeed with 0 Byzantine nodes.")
        self.assertEqual(result.decided_value, "VALUE_Y")
        
        # Check if all phases were covered
        phases = result.messages_per_phase.keys()
        print(f"Phases executed: {list(phases)}")
        self.assertIn("PREPREPARE1", phases)
        self.assertIn("IN_PREPARE1", phases)
        self.assertIn("IN_PREPARE2", phases)
        self.assertIn("OUT_PREPARE", phases)
        self.assertIn("COMMIT", phases)
        self.assertIn("PREPREPARE2", phases)

if __name__ == "__main__":
    unittest.main()
