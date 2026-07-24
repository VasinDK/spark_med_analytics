from dataclasses import dataclass, asdict

@dataclass
class StatsTableSync:
    tables_created: int = 0
    columns_added: int = 0
    columns_deleted: int = 0
    tables_checked: int = 0    

    def to_dict(self) -> dict:
        return asdict(self)