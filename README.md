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
