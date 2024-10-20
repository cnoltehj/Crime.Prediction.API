from pydantic import BaseModel

# Define the structure of the incoming data using Pydantic
class PredictionData(BaseModel):
    Prediction: int
    TrueValue: int
    Algorithm: str
    Scenario: int
    CrimeCategoryCode: str
    ProvinceCode: str
    PoliceStationCode: str
    Quarter: int
    PredictionYear: int

class MerticData(BaseModel):
    Algorithm : str
    Scenario : int
    PredictedYear : int
    MAE : float
    MSE : float
    MAPE : float
    RSquare : float
    ARS : float