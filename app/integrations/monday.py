import json
import logging
import os
from datetime import datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)


class MondayClient:
    endpoint = "https://api.monday.com/v2"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("MONDAY_API_KEY", "")

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

    def _post(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        response = requests.post(self.endpoint, headers=self.headers, json=payload, timeout=30)
        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(str(body["errors"]))
        return body

    def create_item(self, board_id: int, item_name: str, column_values: dict[str, Any]) -> str | None:
        mutation = """
        mutation($board_id: ID!, $item_name: String!, $column_values: JSON!) {
            create_item(board_id: $board_id, item_name: $item_name, column_values: $column_values) {
                id
            }
        }
        """
        try:
            data = self._post(
                mutation,
                {
                    "board_id": str(board_id),
                    "item_name": item_name,
                    "column_values": json.dumps(column_values),
                },
            )
            return data.get("data", {}).get("create_item", {}).get("id")
        except Exception as exc:
            logger.exception("monday_create_item_failed", extra={"error": str(exc)})
            return None

    def get_items(self, board_id: int) -> list[dict[str, Any]]:
        query = """
        query($board_id: ID!) {
            boards(ids: [$board_id]) {
                items_page(limit: 500) {
                    items {
                        id
                        name
                        column_values {
                            id
                            text
                            value
                        }
                    }
                }
            }
        }
        """

        def parse_bool(value: str | None) -> bool:
            if not value:
                return False
            normalized = value.strip().lower()
            return normalized in {"true", "1", "yes", "sim", "done", "concluido", "concluído"}

        def parse_date(value: str | None) -> datetime | None:
            if not value:
                return None
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None

        items_out: list[dict[str, Any]] = []
        try:
            data = self._post(query, {"board_id": str(board_id)})
            boards = data.get("data", {}).get("boards", [])
            if not boards:
                return []

            for item in boards[0].get("items_page", {}).get("items", []):
                email = None
                responded = False
                meeting_scheduled = False
                meeting_done = False
                sale = False
                meeting_scheduled_at = None
                meeting_done_at = None
                sale_at = None

                for col in item.get("column_values", []):
                    col_id = (col.get("id") or "").lower()
                    col_text = col.get("text")

                    if "email" in col_id and col_text:
                        email = col_text
                    if any(key in col_id for key in ["respondeu", "responded", "resposta"]):
                        responded = parse_bool(col_text)
                    if any(key in col_id for key in ["agendada", "scheduled", "reuniao_agendada", "meeting_scheduled"]):
                        meeting_scheduled = parse_bool(col_text)
                    if any(key in col_id for key in ["realizada", "meeting_done", "reuniao_realizada"]):
                        meeting_done = parse_bool(col_text)
                    if any(key in col_id for key in ["venda", "sale", "won"]):
                        sale = parse_bool(col_text)

                    if any(key in col_id for key in ["data_agendada", "meeting_scheduled_at"]):
                        meeting_scheduled_at = parse_date(col_text)
                    if any(key in col_id for key in ["data_realizada", "meeting_done_at"]):
                        meeting_done_at = parse_date(col_text)
                    if any(key in col_id for key in ["data_venda", "sale_at"]):
                        sale_at = parse_date(col_text)

                items_out.append(
                    {
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "email": email,
                        "responded": responded,
                        "meeting_scheduled": meeting_scheduled,
                        "meeting_done": meeting_done,
                        "sale": sale,
                        "meeting_scheduled_at": meeting_scheduled_at,
                        "meeting_done_at": meeting_done_at,
                        "sale_at": sale_at,
                    }
                )
            return items_out
        except Exception as exc:
            logger.exception("monday_get_items_failed", extra={"error": str(exc)})
            return []
