import joblib
import numpy as np
from abc import ABC, abstractmethod
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, VotingRegressor

# --- DESIGN PATTERN: STRATEGY ---
# Allows switching between different surrogate model implementations (RF, GB, NN, etc.)

class SurrogateStrategy(ABC):
    """Abstract base class for surrogate model strategies."""
    
    @abstractmethod
    def train(self, X: np.ndarray, y: np.ndarray):
        pass
    
    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        pass
    
    @abstractmethod
    def save(self, path: str):
        pass
    
    @abstractmethod
    def load(self, path: str):
        pass

class TreeEnsembleStrategy(SurrogateStrategy):
    """Implementation using an ensemble of tree-based models (Random Forest + Gradient Boosting)."""
    
    def __init__(self, n_estimators: int = 500):
        self.rf = RandomForestRegressor(n_estimators=n_estimators, random_state=42)
        self.gb = GradientBoostingRegressor(random_state=42)
        self.model = VotingRegressor(estimators=[('rf', self.rf), ('gb', self.gb)])
        
    def train(self, X: np.ndarray, y: np.ndarray):
        self.model.fit(X, y)
        
    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict(X)
        
    def save(self, path: str):
        joblib.dump(self.model, path)
        
    def load(self, path: str):
        self.model = joblib.load(path)

# --- DESIGN PATTERN: FACTORY ---
# Creates the appropriate surrogate strategy based on configuration

class SurrogateFactory:
    """Factory for creating surrogate model strategies."""
    
    @staticmethod
    def create_strategy(strategy_type: str, **kwargs) -> SurrogateStrategy:
        if strategy_type == "tree_ensemble":
            return TreeEnsembleStrategy(**kwargs)
        # Add other strategies like 'neural_network' here
        raise ValueError(f"Unknown strategy type: {strategy_type}")
