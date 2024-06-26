import logging
import json
from typing import Any, Optional, Union

import requests
from requests.adapters import HTTPAdapter, Retry

from .errors import ArgumentError

_logger = logging.getLogger(__name__)


class Metabase:

    def __init__(
        self,
        url: str,
        api_key: Optional[str],
        username: Optional[str],
        password: Optional[str],
        session_id: Optional[str],
        skip_verify: bool,
        cert: Optional[Union[str, tuple[str, str]]],
        http_timeout: int,
        http_headers: Optional[dict],
        http_adapter: Optional[HTTPAdapter],
    ):
        self.url = url.rstrip("/")

        self.http_timeout = http_timeout

        self.session = requests.Session()
        self.session.verify = not skip_verify
        self.session.cert = cert

        if http_headers:
            self.session.headers.update(http_headers)

        self.session.mount(
            self.url,
            http_adapter or HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)),
        )

        if api_key:
            self.session.headers["X-API-KEY"] = api_key
        elif username and password:
            session = dict(
                self._api(
                    method="post",
                    path="/api/session",
                    json={"username": username, "password": password},
                )
            )
            self.session.headers["X-Metabase-Session"] = str(session["id"])
        elif session_id:
            _logger.warning(
                "Metabase session ID is deprecated and will be removed in future, use API key or username/password instead"
            )
            self.session.headers["X-Metabase-Session"] = session_id
        else:
            raise ArgumentError("Metabase API key or username/password required")

        _logger.info("Metabase session established")

    def _api(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> Union[dict, list]:
        """Raw API call."""

        if params:
            for key, value in params.items():
                if isinstance(value, bool):
                    params[key] = str(value).lower()

        response = self.session.request(
            method=method,
            url=f"{self.url}{path}",
            params=params,
            timeout=self.http_timeout,
            **kwargs,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            _logger.error("HTTP request failed: %s", response.text)
            raise

        response_json = response.json()
        if "data" in response_json:
            # Since X.40.0 list responses are encapsulated in "data" with pagination parameters
            return response_json["data"]

        return response_json

    def find_database(self, name: str) -> Optional[dict]:
        """Finds database by name attribute or returns none."""
        for api_database in list(self._api("get", "/api/database")):
            if api_database["name"].upper() == name.upper():
                return api_database
        return None

    def sync_database_schema(self, uid: str):
        """Triggers schema sync on a database."""
        self._api("post", f"/api/database/{uid}/sync_schema")

    def get_database_metadata(self, uid: str) -> dict:
        """Retrieves metadata for all tables and fields in a database, including hidden ones."""
        return dict(
            self._api(
                method="get",
                path=f"/api/database/{uid}/metadata",
                params={"include_hidden": True},
            )
        )

    def get_tables(self, lower=True) -> list[dict]:
        """Retrieves all tables for all databases."""
        tables = list(self._api("get", "/api/table"))
        if lower:
            for t in tables:
                t["name"] = t["name"].lower()
        _logger.debug(
            f"Found the following tables in Metabase: {[t['name'] for t in tables]}"
        )
        return tables

    def get_columns(self, table_id: str, lower=True) -> list[dict]:
        response = self._api("get", f"/api/table/{table_id}/query_metadata")
        columns = list(dict(response)["fields"])
        if lower:
            for c in columns:
                c["name"] = c["name"].lower()
        _logger.debug(
            f"Found columns: {[c['name'] for c in columns]}"
        )
        return columns

    def get_collections(self, exclude_personal: bool) -> list[dict]:
        """Retrieves all collections and optionally filters out personal collections."""
        results = list(
            self._api(
                method="get",
                path="/api/collection",
                params={"exclude-other-user-collections": exclude_personal},
            )
        )
        if exclude_personal:
            results = list(filter(lambda x: not x.get("personal_owner_id"), results))
        return results

    def get_collection_items(
        self,
        uid: str,
        models: list[str],
    ) -> list[dict]:
        """Retrieves collection items of specific types (e.g. card, dashboard, collection)."""
        results = list(
            self._api(
                method="get",
                path=f"/api/collection/{uid}/items",
                params={"models": models},
            )
        )
        results = list(filter(lambda x: x["model"] in models, results))
        return results

    def get_current_user(self) -> dict:
        return dict(self._api("get", "/api/user/current"))

    def find_card(self, uid: str) -> Optional[dict]:
        """Retrieves card (known as question in Metabase UI)."""
        try:
            return dict(self._api("get", f"/api/card/{uid}"))
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                _logger.warning("Card '%s' not found", uid)
                return None
            raise

    def all_cards(self, archived: bool = False):
        return self._api(
            "get", "/api/search", params={"models": "card", "archived": archived}
        )

    def search(self, type: str, archived: bool = False, created_by=None):
        params = {"models": type, "archived": archived}
        if created_by is not None:
            params["created_by"] = created_by
        return self._api("get", "/api/search", params=params)

    def create_card(self, body: dict) -> dict:
        headers = {"Content-Type": "application/json"}
        return dict(
            self._api("post", "/api/card", data=json.dumps(body), headers=headers)
        )

    def update_card(self, id: int, body: dict) -> dict:
        headers = {"Content-Type": "application/json"}
        return dict(
            self._api("put", f"/api/card/{id}", data=json.dumps(body), headers=headers)
        )

    def format_card_url(self, uid: str) -> str:
        """Formats URL link to a card (known as question in Metabase UI)."""
        return f"{self.url}/card/{uid}"

    def find_dashboard(self, uid: int) -> dict:
        return dict(self._api("get", f"/api/dashboard/{uid}"))

    def create_dashboard(self, name: str):
        headers = {"Content-Type": "application/json"}
        return dict(
            self._api(
                "post",
                f"/api/dashboard",
                data=json.dumps({"name": name}),
                headers=headers,
            )
        )

    def update_dashboard(self, id: int, body: dict):
        headers = {"Content-Type": "application/json"}
        return dict(
            self._api(
                "put", f"/api/dashboard/{id}", data=json.dumps(body), headers=headers
            )
        )

    def format_dashboard_url(self, uid: str) -> str:
        """Formats URL link to a dashboard."""
        return f"{self.url}/dashboard/{uid}"

    def create_collection(
        self,
        name: str,
        parent_id: Optional[int] = None,
        description: Optional[str] = None,
    ):
        headers = {"Content-Type": "application/json"}
        body = {"name": name, "parent_id": parent_id, "description": description}
        return dict(
            self._api("post", "/api/collection", data=json.dumps(body), headers=headers)
        )

    def find_user(self, uid: str) -> Optional[dict]:
        """Finds user by ID or returns none."""
        try:
            return dict(self._api("get", f"/api/user/{uid}"))
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                _logger.warning("User '%s' not found", uid)
                return None
            raise

    def update_table(self, uid: str, body: dict) -> dict:
        """Posts update to an existing table."""
        return dict(self._api("put", f"/api/table/{uid}", json=body))

    def update_table_field_order(self, uid: str, body: list) -> list:
        """Posts update to field order of an existing table."""
        return list(self._api("put", f"/api/table/{uid}/fields/order", json=body))

    def update_field(self, uid: str, body: dict) -> dict:
        """Posts an update to an existing table field."""
        return dict(self._api("put", f"/api/field/{uid}", json=body))
