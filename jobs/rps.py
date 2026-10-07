import random
import ray

ray.init()  # Connects to your existing cluster

@ray.remote(num_cpus=0.5)
class Player:
    def __init__(self, name: str):
        self.name = name

    def throw_hand(self) -> tuple:
        # Returns (Player Name, Choice)
        return self.name, random.choice(["Rock", "Paper", "Scissors"])

@ray.remote(num_cpus=0.5)
class Judge:
    def play_round(self, p1_handle, p2_handle) -> str:
        # Asynchronously fetch hands from both player actors simultaneously
        name1, hand1 = ray.get(p1_handle.throw_hand.remote())
        name2, hand2 = ray.get(p2_handle.throw_hand.remote())
        
        if hand1 == hand2:
            return f"Tie! Both chose {hand1}"
        
        # Determine the winner using a simple rules mapping
        beats = {"Rock": "Scissors", "Scissors": "Paper", "Paper": "Rock"}
        winner = name1 if beats[hand1] == hand2 else name2
        return f"{name1} ({hand1}) vs {name2} ({hand2}) -> {winner} wins!"

# 1. Instantiate actors on the existing cluster
p1 = Player.remote("Alice")
p2 = Player.remote("Bob")
referee = Judge.remote()

# 2. Judge calls the players, evaluates the game, and returns the result
result_ref = referee.play_round.remote(p1, p2)

# 3. Get the final output
print(ray.get(result_ref))
