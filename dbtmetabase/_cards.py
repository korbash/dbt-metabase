import dataclasses as dc
import logging
import time
import re
import uuid
import secrets
from abc import ABCMeta, abstractmethod
from typing import (
    Any,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Dict,
    List,
    Literal,
    Iterator,
)

from .errors import MetabaseStateError
from .format import Filter, NullValue, safe_name
from .manifest import (
    DEFAULT_SCHEMA,
    Column,
    Group,
    Manifest,
    Model,
    DashFilter,
    Dashboard,
    Card,
)
from .metabase import Metabase
from ._lockfile import LockFile

_logger = logging.getLogger(__name__)

from typing import List, Dict, Any

from typing import List, Dict, Any

from typing import Dict, Any

COLLECTION = "dbt_cards"
# MODELS_PREFIX = "metabase_"


def _simulate_reverse_gravity(rectangles: Dict[int, Dict[str, Any]], min_row: int = 0):
    """
    Simulate reverse gravity on a dictionary of rectangles.

    Each rectangle is represented as a dictionary with 'size_x', 'size_y', 'row', and 'col' keys.
    """

    # Convert the dictionary to a list of tuples for sorting and processing
    rect_list = list(rectangles.items())

    # Sort rectangles by row, from top-most to bottom-most
    rect_list.sort(key=lambda x: x[1]["row"])

    def can_move_up(rect, new_rectangles):
        """
        Check if the rectangle can move up without overlapping any other rectangle.
        """
        potential_row = rect["row"] - 1
        if potential_row < min_row:
            return False
        for other_id, other in new_rectangles:
            if (
                rect["col"] < other["col"] + other["size_x"]
                and rect["col"] + rect["size_x"] > other["col"]
                and potential_row < other["row"] + other["size_y"]
                and potential_row + rect["size_y"] > other["row"]
            ):
                return False
        return True

    moved = True
    while moved:
        moved = False
        for rect_id, rect in rect_list:
            if can_move_up(rect, rect_list):
                rect["row"] -= 1
                moved = True

    # Convert the list back to a dictionary
    return {rect_id: rect for rect_id, rect in rect_list}


def _generate_rectangles(
    min_row: int = 0, size_y: int = 6, columns: int = 2
) -> Iterator[Dict[str, Any]]:
    size_x = 24 // columns
    column = 0
    row = min_row
    while True:
        rectangle = {
            "size_x": size_x,
            "size_y": size_y,
            "row": row,
            "col": column * size_x,
        }
        yield rectangle
        column += 1
        if column >= columns:
            column = 0
            row += size_y


def get_display_name(name: str):
    converted_name = name.replace("_", " ").title()
    return converted_name


class CardsCreator(metaclass=ABCMeta):

    @property
    @abstractmethod
    def manifest(self) -> Manifest:
        pass

    @property
    @abstractmethod
    def metabase(self) -> Metabase:
        pass

    def update_cards(self, collection: str = COLLECTION, models_prefix: str = ""):
        user_id = self.metabase.get_current_user()["id"]
        dashboards = self.manifest.read_dashboards()
        dbt_clt = self.__create_collection(collection)
        for dash in dashboards:
            dash_clt = self.__create_collection(dash.name, dbt_clt)
            self.__enrich_filters(dash)
            for card in dash.cards.values():
                card.name = card.name.removeprefix(models_prefix)
                card.card_id = self.__find(card.name, user_id, type="card")
                self.__write_card(card, dash.filters, dash_clt)
                card.card_id = self.__find(card.name, user_id, type="card")
            dash_id = self.__find(dash.name, user_id, type="dashboard")
            if dash_id is None:
                self.metabase.create_dashboard(dash.name)
                dash_id = self.__find(dash.name, user_id, type="dashboard")
                _logger.debug(f"created new dash {dash_id} name {dash.name}")
                if dash_id is None:
                    raise (ValueError("dash_id is none"))
            card_sizes = self.__get_card_sizes(dash_id)
            if len(dash.tabs) == 0:
                dash.tabs = {"main": list(dash.cards.keys())}
            if len(dash.tabs_order) == 0:
                dash.tabs_order = list(dash.tabs.keys())
            new_card_sizes = self.__generate_card_sizes(dash, card_sizes)
            self.__write_dash(dash, dash_id, new_card_sizes)

    def __create_collection(
        self,
        name: str,
        parent_id: Optional[int] = None,
        description: Optional[str] = None,
    ):
        parent_str = "" if parent_id is None else str(parent_id)
        cls = self.metabase.search("collection")
        cl_ids = [
            c["id"]
            for c in cls
            if c["name"] == name and c["location"].split("/")[-2] == parent_str
        ]
        if len(cl_ids) == 0:
            _logger.debug(f"creating new collection {name}")
            cl_id = self.metabase.create_collection(name, parent_id, description)["id"]
        elif len(cl_ids) == 1:
            _logger.debug(f"collection {name} exist")
            cl_id = cl_ids[0]
        else:
            raise ValueError(
                f"finded {len(cl_ids)} with name {name} and parent {parent_id}"
            )
        return int(cl_id)

    def __enrich_filters(self, dash: Dashboard) -> dict[str, DashFilter]:
        filters = dash.filters
        tables = self.metabase.get_tables()
        # Iterate through each filter and update it with additional metadata
        for filter in filters.values():
            # Find the table that matches the model_name of the current filter
            table = next(t for t in tables if t["name"] == filter.model_name)
            filter.db_id = table["db_id"]
            columns = self.metabase.get_columns(table["id"])
            column = next(c for c in columns if c["name"] == filter.column_name)

            filter.column_id = column["id"]
            filter.column_effective_type = column["effective_type"]
            filter.column_base_type = column["base_type"]
        return filters

    def __find(
        self, name: str, user_id: str, type: Literal["card", "dashboard"]
    ) -> int:
        dbt_cards = self.metabase.search(type, created_by=user_id)  # type: ignore
        return next((c["id"] for c in dbt_cards if c["name"] == name), -1)

    def __write_card(
        self,
        card: Card,
        filters: dict[str, DashFilter],
        collection: Optional[int] = None,
    ):

        def tag(filter: DashFilter, name, id):
            dim = ["field", filter.column_id, {"base-type": filter.column_base_type}]
            template_tag = {
                "type": "dimension",
                "name": name,
                "id": id,  # str(uuid.uuid4())
                "default": filter.default,
                "dimension": dim,
                "widget-type": filter.widget_type,
                "display-name": get_display_name(name),
            }
            return {name: template_tag}

        def param(filter: DashFilter, name, id):
            return {
                "id": id,
                "type": filter.widget_type,
                "target": ["dimension", ["template-tag", name]],
                "slug": name,
                "name": get_display_name(name),
            }

        db = set(filters[f].db_id for f in card.filters)
        if len(db) > 1:
            raise ValueError("Multiple databases detected")
        db = db.pop()

        tags = {}
        params = []
        for f in card.filters:
            id = str(uuid.uuid4())
            tags.update(tag(filters[f], f, id))
            params.append(param(filters[f], f, id))
        dataset_query = {
            "database": db,
            "type": "native",
            "native": {"template-tags": tags, "query": card.card_sql},
        }
        if card.card_id != -1:
            _logger.debug(f"updating exist card {card.card_id} name {card.name}")
            data = {
                "name": card.name,
                "type": "question",
                "dataset_query": dataset_query,
                "parameters": params,
                "archived": False,
                "collection_id": collection,
            }
            return self.metabase.update_card(card.card_id, data)
        else:
            _logger.debug(f"creating new card name {card.name}")
            data = {
                "name": card.name,
                "cache_ttl": None,
                "dataset": False,
                "type": "question",
                "dataset_query": dataset_query,
                "display": "table",
                "description": None,
                "visualization_settings": {},
                "parameters": params,
                "parameter_mappings": [],
                "archived": False,
                "enable_embedding": False,
                "embedding_params": None,
                "collection_id": collection,
                "collection_position": None,
                "collection_preview": True,
                "result_metadata": None,
            }
            return self.metabase.create_card(data)

    def __get_card_sizes(self, dash_id: int):
        dash = self.metabase.find_dashboard(dash_id)
        keys = ["size_x", "size_y", "row", "col", "dashboard_tab_id"]
        card_sizes = {}
        for card in dash["dashcards"]:
            if "card_id" in card:
                id = card["card_id"]
                c = {key: card[key] for key in keys if key in card}
                card_sizes[id] = c
        return card_sizes

    def __generate_card_sizes(self, dash: Dashboard, card_sizes: dict):
        resut_sizes = {}
        for tab, tab_name in enumerate(dash.tabs_order):
            ids = [dash.cards[c].card_id for c in dash.tabs[tab_name]]
            sizes_id = set(
                k for k, v in card_sizes.items() if v["dashboard_tab_id"] == tab
            )
            exist_sizes = {key: card_sizes[key] for key in ids if key in sizes_id}
            _logger.debug(f"exist_rectangels: {exist_sizes} for tab {tab} ({tab_name})")
            modify_exist = _simulate_reverse_gravity(exist_sizes)
            _logger.debug(f"modified_rectan: {modify_exist} for tab {tab} ({tab_name})")
            if len(modify_exist) > 0:
                max_row = max(
                    rect["row"] + rect["size_y"] for rect in modify_exist.values()
                )
            else:
                max_row = 0
            grid_iterator = _generate_rectangles(min_row=max_row)
            new_sizes = {
                key: next(grid_iterator) for key in ids if key not in exist_sizes
            }
            _logger.debug(f"new_rectangels: {new_sizes} for tab {tab} ({tab_name})")
            resut_sizes.update(modify_exist)
            resut_sizes.update(new_sizes)
        return resut_sizes

    def __write_dash(self, dash: Dashboard, dash_id: int, card_sizes):
        prm = {}
        dashcards = []
        tabs = []
        i = 0
        for tab, tab_name in enumerate(dash.tabs_order):
            tabs.append({"id": tab, "name": tab_name})
            for card_name in dash.tabs[tab_name]:
                card = dash.cards[card_name]
                prm_card = []
                for f in card.filters:
                    filter = dash.filters[f]
                    if f in prm:
                        id = prm[f]["id"]
                    else:
                        id = secrets.token_hex(4)
                        p2 = {
                            "name": get_display_name(f),
                            "slug": f,
                            "id": id,
                            "type": filter.widget_type,
                            "sectionId": filter.widget_type.split("/")[0],
                            "default": filter.default,
                        }
                        prm.update({f: p2})
                    p = {
                        "parameter_id": id,
                        "card_id": card.card_id,
                        "target": ["dimension", ["template-tag", f]],
                    }
                    prm_card.append(p)
                size = card_sizes[card.card_id]
                d = {
                    "id": i,
                    "card_id": card.card_id,
                    "dashboard_tab_id": tab,
                    "row": size["row"],
                    "col": size["col"],
                    "size_x": size["size_x"],
                    "size_y": size["size_y"],
                    "parameter_mappings": prm_card,
                }
                dashcards.append(d)
                i += 1
        if dash.filters_order is None:
            parameters = list(prm.values())
        else:
            parameters = [prm[f] for f in dash.filters_order]
        data = {
            "dashcards": dashcards,
            "parameters": parameters,
            "name": dash.name,
            "description": dash.description,
            "archived": False,
            "can_write": True,
            "tabs": tabs,
            # 'enable_embedding': False,
            # 'collection_id': None,
            # 'show_in_getting_started': False,
            # 'width': 'fixed',
            # 'auto_apply_filters': True,
        }
        _logger.debug(f"updating dash {dash_id} name {dash.name}")
        _logger.debug(data)
        return self.metabase.update_dashboard(dash_id, data)
