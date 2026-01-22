from typing import Dict, List, Optional

from pydantic import BaseModel


class ComponentVersion(BaseModel):
    version_filter: Optional[str]


class CheckDescription(BaseModel):
    helmUrl: str
    releaseName: str
    version: str

    class Config:
        populate_by_name = True


class Check(BaseModel):
    type: str
    description: CheckDescription


class Scenario(BaseModel):
    type: str
    k8s_template: str
    checks: List[str]
    ttl: int
    operator: ComponentVersion
    components: Dict[str, ComponentVersion]

    class Config:
        populate_by_name = True
        frozen = True


class ScenarioConfig(BaseModel):
    scenarios: Dict[str, Scenario]
    checks: Dict[str, Check]
