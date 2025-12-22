import pandas as pd
from typing import List, Dict
from nbft.models import RunConfig
from nbft.pbft_sim import PBFTSimulator
from nbft.nbft_sim import NBFTSimulator
from db.repository import Repository

class ExperimentRunner:
    """
    Experiment Runner
    Orchestrates batch simulations to compare PBFT and NBFT results
    """
    
    def __init__(self):
        self.repo = Repository()

    async def run_single(self, config: RunConfig, save: bool = False):
        if config.algorithm == "PBFT":
            sim = PBFTSimulator(config)
        else:
            sim = NBFTSimulator(config)
            
        result = await sim.run()
        
        if save:
            self.repo.save_run(config, result)
            
        return result

    async def run_batch_byzantine_sweep(self, n: int, m: int, max_f: int, trials: int = 5):
        """
        Sweeps byzantine count from 0 to max_f
        Reproduces Success Rate vs Byzantine Nodes plots
        """
        results = []
        
        # Test both algos
        for algo in ["PBFT", "NBFT"]:
            for f_count in range(max_f + 1):
                success_count = 0
                avg_msgs = 0
                
                for _ in range(trials):
                    config = RunConfig(
                        algorithm=algo,
                        n=n,
                        m=m,
                        actual_byzantine=f_count
                    )
                    res = await self.run_single(config, save=True)
                    if res.success: success_count += 1
                    avg_msgs += res.total_messages
                    
                start_rate = (success_count / trials) * 100
                avg_msgs /= trials
                
                results.append({
                    "Algorithm": algo,
                    "ByzantineNodes": f_count,
                    "SuccessRate": start_rate,
                    "AvgMessages": avg_msgs
                })
        
        return pd.DataFrame(results)

    async def run_complexity_analysis(self, n_range: List[int], m: int):
        """
        Compares message complexity
        """
        results = []
        for n in n_range:
            for algo in ["PBFT", "NBFT"]:
                config = RunConfig(algorithm=algo, n=n, m=m, actual_byzantine=0)
                res = await self.run_single(config)
                results.append({
                    "Algorithm": algo,
                    "N": n,
                    "Messages": res.total_messages
                })
        return pd.DataFrame(results)
