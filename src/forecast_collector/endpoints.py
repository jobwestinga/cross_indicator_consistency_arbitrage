from __future__ import annotations

from .models import RequestSpec


class ForecastTraderEndpoints:
    def __init__(self, public_prefix: str, exchange: str) -> None:
        self.public_prefix = public_prefix.rstrip("/")
        self.exchange = exchange

    def market(self, underlying_conid: int) -> RequestSpec:
        return RequestSpec(
            endpoint_name="market",
            path=f"{self.public_prefix}/forecasttrader/contract/market",
            params={"underlyingConid": underlying_conid, "exchange": self.exchange},
        )

    def contract_details(self, conid: int) -> RequestSpec:
        return RequestSpec(
            endpoint_name="contract_details",
            path=f"{self.public_prefix}/forecasttrader/contract/details",
            params={"conid": conid},
        )

    def history(self, conid: int, period: str) -> RequestSpec:
        return RequestSpec(
            endpoint_name="history",
            path=f"{self.public_prefix}/hmds/forecastContract",
            params={
                "conid": conid,
                "period": period,
                "exchange": self.exchange,
                "secType": "FOP",
            },
        )

    def open_interest(self, conid: int) -> RequestSpec:
        return RequestSpec(
            endpoint_name="open_interest",
            path=f"{self.public_prefix}/event-contract/market-open-interest",
            params={"id": conid},
        )

    def projected_probabilities(self, underlying_conid: int) -> RequestSpec:
        return RequestSpec(
            endpoint_name="projected_probabilities",
            path=f"{self.public_prefix}/event-contract/projected-probabilities",
            params={"und_conid": underlying_conid},
        )
