"""
Моделі даних для DER/VPP системи
"""
from datetime import datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field, model_validator

AssetType = Literal['solar', 'wind', 'battery', 'diesel']
EventType = Literal['ASSET_DISPATCHED', 'BID_SUBMITTED', 'CAPACITY_CHANGED']


class DERReading(BaseModel):
    """Телеметричні дані від DER активу"""
    asset_id: str
    asset_type: AssetType
    timestamp: datetime
    power_output: float = Field(description="Поточна вихідна потужність (кВт). Для батарей може бути негативним (зарядка)")
    available_capacity: float = Field(ge=0, description="Доступна потужність (кВт)")
    soc: Optional[float] = Field(None, ge=0, le=100, description="Рівень заряду батареї (%)")
    fuel_level: Optional[float] = Field(None, ge=0, le=100, description="Рівень палива (%)")
    forecasted_output: float = Field(description="Прогнозована вихідна потужність (кВт). Для батарей може бути негативним")
    
    @model_validator(mode='after')
    def validate_power_values(self):
        """Валідація power_output та forecasted_output: для батарей дозволяємо негативні значення"""
        # Для батарей дозволяємо негативні значення (зарядка)
        if self.asset_type == 'battery':
            return self
        
        # Для інших типів вимагаємо невід'ємні значення
        if self.power_output < 0:
            raise ValueError(f'power_output must be >= 0 for {self.asset_type} assets, got {self.power_output}')
        if self.forecasted_output < 0:
            raise ValueError(f'forecasted_output must be >= 0 for {self.asset_type} assets, got {self.forecasted_output}')
        
        return self


class DEREvent(BaseModel):
    """Подія в системі Event Sourcing"""
    event_id: str
    event_type: EventType
    asset_id: str
    timestamp: datetime
    payload: dict
    metadata: Optional[dict] = None


class PortfolioAggregate(BaseModel):
    """Агреговані дані VPP portfolio за вікно"""
    window_start: datetime
    window_end: datetime
    total_capacity: float = Field(ge=0, description="Загальна потужність (кВт)")
    available_capacity: float = Field(ge=0, description="Доступна потужність (кВт)")
    dispatch_margin: float = Field(description="Маржа диспетчеризації (кВт)")
    total_output: float = Field(ge=0, description="Загальна вихідна потужність (кВт)")
    asset_count: int = Field(ge=0, description="Кількість активів")
    asset_type_breakdown: dict[str, int] = Field(default_factory=dict)


class Anomaly(BaseModel):
    """Виявлена аномалія"""
    anomaly_id: str
    asset_id: str
    timestamp: datetime
    anomaly_type: str
    severity: Literal['low', 'medium', 'high', 'critical']
    description: str
    value: float
    expected_range: tuple[float, float]
    z_score: Optional[float] = None

