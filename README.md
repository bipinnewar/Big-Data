# Reinforcement Learning for Autonomous Navigation

## Overview

This project demonstrates the implementation of a **Q-Learning Reinforcement Learning agent** capable of autonomously navigating a custom 5×5 Grid World environment. The agent learns an optimal navigation policy through trial-and-error interactions, balancing exploration and exploitation using an epsilon-greedy strategy.

The environment contains obstacles, a teleportation state, and a goal state with rewards. Through continuous training, the agent learns the most efficient path to maximize cumulative rewards while minimizing unnecessary actions.

---

## Features

* Custom 5×5 Grid World environment
* Q-Learning implementation from scratch
* Epsilon-Greedy exploration strategy
* Dynamic exploration decay
* Obstacle-aware path planning
* Teleportation transition state
* State-Action Value (Q-Table) visualization
* Learned policy and value heatmap generation
* Training performance monitoring
* Reward and efficiency analysis

---

## Problem Description

The agent must navigate a constrained environment and reach the goal state while maximizing rewards.

### Environment Configuration

* Grid Size: 5 × 5
* Start State: (1, 0)
* Goal State: (4, 4)
* Teleport State: (1, 3) → (3, 3)
* Obstacles:

  * (2,2)
  * (2,3)
  * (3,2)
  * (2,4)

### Available Actions

* Up
* Down
* Left
* Right

---

## Reinforcement Learning Approach

### Q-Learning

Q-Learning is a model-free reinforcement learning algorithm that learns the optimal action-value function:

```text
Q(s,a) ← Q(s,a) + α [r + γ max Q(s',a') − Q(s,a)]
```

Where:

* Q(s,a) = Action value
* α = Learning Rate
* γ = Discount Factor
* r = Immediate Reward
* s' = Next State

---

## Exploration Strategy

The agent uses an **Epsilon-Greedy Policy**:

* Random exploration with probability ε
* Greedy action selection with probability (1 − ε)

### Epsilon Decay

```text
Initial ε = 1.0
Minimum ε = 0.01
```

This allows the agent to explore early and exploit learned knowledge later in training.

---

## Hyperparameters

| Parameter           | Value |
| ------------------- | ----- |
| Learning Rate (α)   | 0.1   |
| Discount Factor (γ) | 0.9   |
| Initial Epsilon     | 1.0   |
| Minimum Epsilon     | 0.01  |
| Training Episodes   | 300   |

---

## Technologies Used

* Python
* NumPy
* Matplotlib
* Reinforcement Learning
* Q-Learning
* Object-Oriented Programming

---

## Project Structure

```text
Reinforcement-Learning-GridWorld/
│
├── src/
│   ├── environment.py
│   ├── q_learning_agent.py
│   ├── train.py
│   └── visualization.py
│
├── outputs/
│   ├── reward_curve.png
│   ├── efficiency_curve.png
│   ├── policy_heatmap.png
│   └── value_map.png
│
├── requirements.txt
└── README.md
```

---

## Results

### Learning Performance

The agent demonstrated three learning phases:

#### Phase 1 – Exploration

* Random actions dominate.
* Rewards fluctuate significantly.
* Frequent collisions and inefficient paths.

#### Phase 2 – Learning

* Agent discovers high-value states.
* Cumulative rewards steadily increase.
* Path efficiency improves.

#### Phase 3 – Convergence

* Stable policy emerges.
* Agent consistently reaches the goal.
* Near-optimal navigation achieved.

---

## Key Outcomes

* Successfully learned optimal navigation policy.
* Adapted to obstacles and teleportation mechanics.
* Reduced average steps per episode throughout training.
* Generated interpretable policy and value visualizations.
* Demonstrated convergence of Q-values and stable learning behavior.

---

## Visualizations

The project generates:

* Cumulative Reward Curve
* Average Steps per Episode
* Learned Policy Heatmap
* State Value Map
* Optimal Path Visualization

These visualizations provide insight into the learning process and final decision-making strategy.

---

## Future Improvements

* Implement SARSA for algorithm comparison
* Expand environment size (10×10, 20×20)
* Introduce dynamic obstacles
* Implement Deep Q-Networks (DQN)
* Add Double DQN and Dueling DQN architectures
* Support continuous state spaces
* Develop interactive visualization dashboard

---

## Learning Outcomes

This project demonstrates practical understanding of:

* Reinforcement Learning fundamentals
* Markov Decision Processes (MDPs)
* Exploration vs Exploitation trade-off
* Q-Learning algorithms
* Policy optimization
* Reward-based learning systems
* Autonomous agent navigation

---

## Author

**Bipin Shrestha**

MSc Computer Science and Technology
Ulster University

---

## License

This project is intended for educational and research purposes.
