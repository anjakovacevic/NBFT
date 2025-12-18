import gradio as gr
import pandas as pd
import matplotlib.pyplot as plt
import os
from nbft.models import RunConfig
from experiments.runner import ExperimentRunner
from db.repository import Repository
from nbft.analysis import Analysis

# Setup
runner = ExperimentRunner()
repo = Repository()

def plot_nbft_trace(trace, n, m):
    """
    Generates a sequence diagram matching Figure 1 of the paper.
    """
    if not trace: 
        return None
        
    height = max(10, n * 0.6)
    fig, ax = plt.subplots(figsize=(14, height))
    
    # Setup Y-axis (Nodes)
    # Highlight Reps. We need to identify them. 
    # Since we don't have the simulator object here, we infer from trace or re-calculate.
    # Re-calculating consistent hash groupings locally for visualization context.
    from nbft.models import Node
    from nbft.consistent_hash import ConsistentHashing
    
    temp_nodes = [Node(i, "") for i in range(n)]
    ch = ConsistentHashing(temp_nodes, m)
    groups, node_group_map = ch.form_groups(0)
    reps = {g.representative_id for g in groups}
    global_primary = ch.get_global_primary(0)
    
    # Sort nodes by Group ID for better visualization
    # We want Group 0 nodes at the top, then Group 1, etc.
    nodes_by_group = {}
    for g in groups:
        nodes_by_group[g.group_id] = sorted(g.members)
        
    sorted_node_list = []
    for gid in sorted(nodes_by_group.keys()):
        sorted_node_list.extend(nodes_by_group[gid])
        
    # Map real node ID to Y-axis position (index in sorted list)
    y_pos_map = {node_id: idx for idx, node_id in enumerate(sorted_node_list)}
    
    # Plot horizontal lines for nodes
    current_group = -1
    for idx, i in enumerate(sorted_node_list):
        # Determine group for visual separation
        group_id = node_group_map[i]
        
        # Add visual separator between groups
        if group_id != current_group:
            if current_group != -1:
                # Draw a thin line between groups
                 ax.hlines(y=idx-0.5, xmin=0, xmax=max(t['arrival'] for t in trace)*1.05, colors='lightgray', linestyles='dotted', linewidth=1.0)
            current_group = group_id
            
        color = 'black'
        linewidth = 1
        label = f"Node {i} (G{group_id})"
        
        if i == global_primary:
            color = 'blue'
            linewidth = 2.5
            label += " [Global Pri]"
            ax.hlines(y=idx, xmin=0, xmax=max(t['arrival'] for t in trace)*1.05, colors=color, linestyles='dashed', linewidth=linewidth)
        elif i in reps:
            color = 'black'
            linewidth = 2.5
            label += " [Rep]"
            ax.hlines(y=idx, xmin=0, xmax=max(t['arrival'] for t in trace)*1.05, colors=color, linewidth=linewidth)
        else:
            ax.hlines(y=idx, xmin=0, xmax=max(t['arrival'] for t in trace)*1.05, colors='gray', linewidth=0.5)
            
        ax.text(-0.02, idx, label, fontsize=8, va='center', ha='right', transform=ax.get_yaxis_transform())
    
    # Redefine total sorted nodes count for Y-axis limit
    total_plotted_nodes = len(sorted_node_list)

    # Draw Client Line
    ax.hlines(y=-1, xmin=0, xmax=max(t['arrival'] for t in trace)*1.05, colors='green', linewidth=2.0)
    ax.text(-0.02, -1, "Client", fontsize=9, va='center', ha='right', fontweight='bold', transform=ax.get_yaxis_transform())

    # Map message types to colors
    type_colors = {
        "REP_PRE_PREPARE": "blue",
        "GROUP_PRE_PREPARE": "green",
        "GROUP_VOTE": "orange",
        "GROUP_RESULT": "lime",
        "ALARM": "black",
        "REP_PREPARE": "purple",
        "REP_COMMIT": "brown",
        "FINAL_DECISION": "red",
        "REPLY": "cyan",
        "VIEW_CHANGE": "magenta", 
    }

    # Plot Messages
    for msg in trace:
        t_start = msg['time']
        t_end = msg['arrival']
        src_id = msg['sender']
        dst_id = msg['receiver']
        mtype = msg['type']
        
        # Map IDs to plotted Y positions
        src = y_pos_map.get(src_id, src_id) # Fallback for Client (-1) or unknown
        dst = y_pos_map.get(dst_id, dst_id)
        
        # Handle Client: -1 needs to be placed separately or mapped
        # In this plot, Client is at Y = -1. Let's keep it.
        # But if we use y_pos_map which are 0..N-1, then -1 is below.
        # However, our y_pos_map is 0-indexed.
        # Usually Client is visually separated. 
        # The previous code plotted Client line at y=-1.
        # So keeping src/dst as -1 works if we don't map it.
        
        color = type_colors.get(mtype, 'gray')
        
        # Draw arrow
        ax.annotate("",
                    xy=(t_end, dst), xycoords='data',
                    xytext=(t_start, src), textcoords='data',
                    arrowprops=dict(arrowstyle="->", color=color, alpha=0.6, lw=1))

    ax.set_title("NBFT Message Sequence Chart")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Node ID")
    ax.set_yticks([]) # Hide standard y-ticks
    ax.invert_yaxis() # Node 0 at top
    
    # Add Legend for message types
    from matplotlib.lines import Line2D
    legend_elements = [Line2D([0], [0], color=c, lw=2, label=t) for t, c in type_colors.items()]
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1, 1), fontsize='small', title="Message Types")
    
    plt.tight_layout()
    return fig

async def run_single_simulation(algo, n, m, bad_nodes):
    try:
        # Validation
        if algo == "NBFT":
            if m < 1: return "Error: Groups (m) must be >= 1", None
            if int(n) < int(m): return f"Error: Number of groups (m={int(m)}) cannot exceed total nodes (n={int(n)}).", None
            pass

        config = RunConfig(
            algorithm=algo,
            n=int(n),
            m=int(m),
            actual_byzantine=int(bad_nodes)
        )
        
        result = await runner.run_single(config, save=True)
        
        output_text = f"Success: {result.success}\n"
        output_text += f"Time: {result.consensus_time:.4f}s\n"
        output_text += f"Total Messages: {result.total_messages}\n"
        output_text += f"Decided Value: {result.decided_value}\n"
        if result.byzantine_nodes:
            output_text += f"Byzantine Nodes: {result.byzantine_nodes}\n"
        output_text += f"Phases: {result.messages_per_phase}\n"
        output_text += "\nLOGS \n" + "\n".join(result.logs[-25:])
        
        fig = None
        if algo == "NBFT" and result.message_trace:
            fig = plot_nbft_trace(result.message_trace, int(n), int(m))
        
        return output_text, fig
    except Exception as e:
        import traceback
        return f"SIMULATION ERROR:\n{str(e)}\n\n{traceback.format_exc()}", None

async def run_batch_experiment(n, m, max_f, trials):
    df = await runner.run_batch_byzantine_sweep(int(n), int(m), int(max_f), int(trials))
    
    # Plotting Success Rate
    fig1 = plt.figure()
    for algo in df["Algorithm"].unique():
        subset = df[df["Algorithm"] == algo]
        plt.plot(subset["ByzantineNodes"], subset["SuccessRate"], label=algo, marker='o')
    
    plt.xlabel("Number of Byzantine Nodes")
    plt.ylabel("Success Rate (%)")
    plt.title(f"Fault Tolerance Comparison (n={n})")
    plt.legend()
    plt.grid(True)
    
    # Plotting Messages
    fig2 = plt.figure()
    for algo in df["Algorithm"].unique():
        subset = df[df["Algorithm"] == algo]
        plt.plot(subset["ByzantineNodes"], subset["AvgMessages"], label=algo, marker='x')

    plt.xlabel("Number of Byzantine Nodes")
    plt.ylabel("Avg Total Messages")
    plt.title("Communication Overhead")
    plt.legend()
    plt.grid(True)

    return fig1, fig2, df

def load_history():
    runs = repo.get_all_runs()
    return pd.DataFrame(runs)

# --- UI Definition ---

with gr.Blocks(title="NBFT Simulator") as demo:
    gr.Markdown(
        """
        # NBFT Simulator
        ### Overview of Consensus Protocols
        *   **Distributed Systems**: A network of autonomous computers that communicate to achieve a common goal. Use consensus algorithms to agree on data values.
        *   **PBFT (Practical Byzantine Fault Tolerance)**: A robust consensus algorithm that tolerates up to *f* malicious (Byzantine) nodes in a network of *3f+1* nodes. Ideally secure but suffers from O(n²) complexity, limiting scalability.
        *   **NBFT (Node-grouped BFT)**: An optimized algorithm that partitions the network into groups. It uses a two-level consensus mechanism (Intra-group & Inter-group) to reduce complexity to approx O(n), enabling better scalability for large networks.
        """
    )
    
    with gr.Tabs():
        # TAB 1: Single Run
        # TAB 1: Single Run
        with gr.TabItem("Single Simulation"):
            # Top Row: Configuration and Text Results
            with gr.Row():
                with gr.Column(scale=1):
                    algo_input = gr.Radio(["PBFT", "NBFT"], label="Algorithm", value="NBFT")
                    n_input = gr.Number(label="Total Nodes (n)", value=20)
                    m_input = gr.Number(label="Groups (m) [NBFT only]", value=4)
                    bad_input = gr.Number(label="Byzantine Nodes", value=0)
                    btn_run = gr.Button("Run Simulation", variant="primary")
                
                with gr.Column(scale=1):
                    output_log = gr.Textbox(label="Result & Logs", lines=12)

            # Bottom Row: Full Width Visualization
            with gr.Row():
                viz_output = gr.Plot(label="Communication Visualization (NBFT Only)")
            
            btn_run.click(run_single_simulation, [algo_input, n_input, m_input, bad_input], [output_log, viz_output])

        # TAB 2: Batch Experiments
        with gr.TabItem("Batch Experiments"):
            gr.Markdown("Compare NBFT vs PBFT performance and fault tolerance.")
            with gr.Row():
                batch_n = gr.Number(label="Fixed N", value=50)
                batch_m = gr.Number(label="Fixed M", value=4)
                batch_max_f = gr.Number(label="Sweep Byzantine up to", value=15)
                batch_trials = gr.Number(label="Trials per point", value=3)
                btn_batch = gr.Button("Run Batch Experiment")
            
            with gr.Row():
                plot_success = gr.Plot(label="Success Rate")
                plot_msgs = gr.Plot(label="Message Complexity")
            
            data_table = gr.Dataframe(label="Experiment Data")
            
            btn_batch.click(run_batch_experiment, 
                           [batch_n, batch_m, batch_max_f, batch_trials], 
                           [plot_success, plot_msgs, data_table])

        # TAB 3: History
        with gr.TabItem("Saved History"):
            btn_refresh = gr.Button("Refresh Data")
            history_table = gr.Dataframe()
            btn_refresh.click(load_history, inputs=None, outputs=history_table)

if __name__ == "__main__":
    demo.launch()
