from __future__ import annotations

from .models import HealthReport
from .repository import CollectorRepository


class HealthReporterService:
    def __init__(self, repository: CollectorRepository) -> None:
        self.repository = repository

    def report(self) -> HealthReport:
        return self.repository.get_health_report()
