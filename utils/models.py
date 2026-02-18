from typing import TypeAlias
from utils.schemas import SignalSchema, ScoreSchema, AlphaSchema, UniverseSchema
import dataframely as dy

Signals: TypeAlias = dy.DataFrame[SignalSchema]
Scores: TypeAlias = dy.DataFrame[ScoreSchema]
Alphas: TypeAlias = dy.DataFrame[AlphaSchema]
Universe: TypeAlias = dy.DataFrame[UniverseSchema]

