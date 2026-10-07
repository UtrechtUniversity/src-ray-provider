import gymnasium as gym
from ray.rllib.env.multi_agent_env import MultiAgentEnv

import gymnasium as gym
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class RockPaperScissors(MultiAgentEnv):
    def __init__(self, config=None):
        super().__init__()
        config = config or {}
        self.max_steps = config.get("max_steps", 10)
        self.possible_agents = ["agent_1", "agent_2"]
        self.agents = list(self.possible_agents)

        # 0 = Rock, 1 = Paper, 2 = Scissors
        self.action_spaces = {a: gym.spaces.Discrete(3) for a in self.possible_agents}
        # Opponent's last action, or 3 on the first step
        self.observation_spaces = {a: gym.spaces.Discrete(4) for a in self.possible_agents}

        self._t = 0

    def reset(self, *, seed=None, options=None):
        self._t = 0
        self.agents = list(self.possible_agents)
        obs = {"agent_1": 3, "agent_2": 3}
        return obs, {}

    def step(self, action_dict):
        a1 = action_dict["agent_1"]
        a2 = action_dict["agent_2"]

        if a1 == a2:
            r1, r2 = 0, 0
        elif (a1, a2) in [(0, 2), (1, 0), (2, 1)]:
            r1, r2 = 1, -1
        else:
            r1, r2 = -1, 1

        self._t += 1
        done = self._t >= self.max_steps

        obs = {"agent_1": a2, "agent_2": a1}
        rewards = {"agent_1": r1, "agent_2": r2}
        terminateds = {"agent_1": done, "agent_2": done, "__all__": done}
        truncateds = {"agent_1": False, "agent_2": False, "__all__": False}
        infos = {}

        return obs, rewards, terminateds, truncateds, infos

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy.policy import PolicySpec

config = (
    PPOConfig()
    .environment(RockPaperScissors, env_config={"max_steps": 10})
    .multi_agent(
        policies={"p1": PolicySpec(), "p2": PolicySpec()},
        policy_mapping_fn=lambda agent_id, *a, **kw: "p1" if agent_id == "agent_1" else "p2",
    )
)
algo = config.build()
for i in range(5):
    result = algo.train()
    print(i, result.get("env_runners", {}).get("episode_return_mean"))
