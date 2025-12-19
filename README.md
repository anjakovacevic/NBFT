# NBFT Simulator

This project implements a simulation of the NBFT (Node-grouped Byzantine Fault Tolerance) consensus algorithm proposed by the paper Improved Fault Tolerant Consensus Based on the PBFT Algorithm.

## Installation

1. Clone or download this repository.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Running the Simulation

Start the application by running:

```bash
python app.py
```

This will launch a local web server (Gradio interface). Open the provided URL (typically `http://127.0.0.1:7860`) in your browser to interact with the simulator.

## Features

- **Single Simulation**: Run individual consensus rounds, visualize message flow, and inspect logs.
- **Batch Experiments**: Run multiple trials to analyze success rates and communication complexity with varying numbers of faulty nodes.
- **History**: View results from previous runs.

## Unit Testing

The project includes two comprehensive test suites to verify both the consensus logic and the security models.

### 1. Node Communication & Byzantine Behavior
Located in `tests/test_node_communication.py`, these tests verify the system's resilience against malicious actors:

- **Honest Behavior**: Ensures messages remain untampered.
- **Silent Failure**: Verifies that messages are dropped when a node enters a silent state.
- **Equivocation**: Shows how a node can send conflicting information to different peers (detected via digest mismatches).
- **Bad Aggregator**: Demonstrates a representative node corrupting group decisions.
- **Fraud Detection**: Validates the logic where honest members detect inconsistent results from their representatives (Algorithm 1: Watchdog).
- **Cryptographic Verification**: Confirms that digital signatures fail if the message digest is tampered with.

### 2. NBFT Protocol Logic
Located in `tests/test_nbft_protocol.py`, these tests verify the mathematical and structural correctness of the algorithm:

- **Consistent Hashing Determinism**: Ensures group formation is stable for a given view.
- **View Change Rotation**: Verifies that the primary and group assignments rotate correctly between views to ensure fairness.
- **Mathematical Thresholds**: Validates the paper's formulas for group size ($R$), intra-group tolerance ($w$), and inter-group tolerance ($E$).
- **End-to-End Consensus**: Runs a full asynchronous simulation of the NBFT phases (Pre-prepare, Intra-group, Inter-group, Commit) to confirm successful agreement.

### How to Run Tests

To run all tests:
```bash
python -m unittest discover tests
```

To run a specific test suite:
```bash
# Byzantine Behavior tests
python -m unittest tests/test_node_communication.py

# Protocol Logic tests
python -m unittest tests/test_nbft_protocol.py
```
