from enum import Enum

class ModelStep(str, Enum):
    staging = "staging"
    intermediate = "intermediate"
    marts = "marts"
    bespoke = "bespoke"
