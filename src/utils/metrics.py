from dataclasses import dataclass, asdict

@dataclass
class MetricsValidate:
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    error_percent: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)
    
@dataclass
class StatsTableSync:
    tables_created: int = 0
    columns_added: int = 0
    columns_deleted: int = 0
    tables_checked: int = 0    

    def to_dict(self) -> dict:
        return asdict(self)
