from __future__ import annotations

import dataclasses as dc
import json
import logging
import re
from enum import Enum
from pathlib import Path
from typing import (
    Iterable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Union,
)
from .format import NullValue

_logger = logging.getLogger(__name__)

# Namespace for meta fields, e.g. metabase.field
_META_NS = "metabase"
# Allowed namespace fields
_COMMON_META_FIELDS = [
    "display_name",
    "visibility_type",
]
# Must be covered by Column attributes
_COLUMN_META_FIELDS = _COMMON_META_FIELDS + [
    "semantic_type",
    "has_field_values",
    "coercion_strategy",
    "number_style",
]
# Must be covered by Model attributes
_MODEL_META_FIELDS = _COMMON_META_FIELDS + [
    "points_of_interest",
    "caveats",
]

_DASH_META_FIELDS = ["filters", "filters_order"]

# Default model schema (only schema in BigQuery)
DEFAULT_SCHEMA = "PUBLIC"

# Foreign key constraint: "schema.model (column)" / "model (column)"
_CONSTRAINT_FK_PARSER = re.compile(r"(?P<model>.+)\s+\((?P<column>.+)\)")


def _replace_conditions_in_query(query: str) -> tuple[str, set[str]]:
    # Define the pattern to match the conditions and capture filter names
    condition_pattern = re.compile(
        r"'__filter__\.(\w+)'\s+is\s+not\s+null", re.MULTILINE
    )

    # Find all matches and extract filter names
    matches = condition_pattern.findall(query)
    filters_found = set(matches)  # Convert list to set to remove duplicates

    # Replace each match individually
    for filter_name in filters_found:
        replacement_pattern = re.compile(
            rf"'__filter__\.{filter_name}'\s+is\s+not\s+null", re.MULTILINE
        )
        replacement = f"{{{{ {filter_name} }}}}"
        query = replacement_pattern.sub(replacement, query)

    return query, filters_found


class Manifest:
    """dbt manifest reader."""

    def __init__(self, target_dir: Union[str, Path]):
        """Reader for compiled dbt manifest.json file.

        Args:
            path (Union[str, Path]): Path to dbt manifest.json (usually under target/).
        """
        self.target_dir = Path(target_dir).expanduser()
        self.path = self.target_dir.joinpath("manifest.json")

    def read_dashboards(self) -> Sequence[Dashboard]:
        with open(self.path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        dashboards = []
        for dash in manifest["exposures"].values():
            if dash["type"] != "dashboard":
                _logger.debug(f"Skipping not dashboard expose {dash['name']}")
                continue
            dashboards.append(self._read_dash(dash, manifest))
        return dashboards

    def read_models(self) -> Sequence[Model]:
        """Reads dbt models in Metabase-friendly format.

        Returns:
            Sequence[Model]: List of dbt models in Metabase-friendly format.
        """

        with open(self.path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        models: MutableSequence[Model] = []

        for node in manifest["nodes"].values():
            if node["resource_type"] != "model":
                continue

            name = node["name"]
            if node["config"]["materialized"] == "ephemeral":
                _logger.debug("Skipping ephemeral model '%s'", name)
                continue

            models.append(self._read_model(manifest, node, Group.nodes))

        for node in manifest["sources"].values():
            if node["resource_type"] != "source":
                continue

            models.append(
                self._read_model(manifest, node, Group.sources, node["source_name"])
            )

        return models

    def _read_dash(self, manifest_dash: Mapping, manifest: Mapping):
        cards = []
        for node_name in manifest_dash["depends_on"]["nodes"]:
            cards.append(self._read_card(manifest["nodes"][node_name]))

        meta = self._scan_fields(
            manifest_dash.get("meta", {}), fields=_DASH_META_FIELDS, ns=_META_NS
        )

        filters = {}
        for f_name, f_data in meta["filters"].items():
            f_model, f_col = f_data["column"].split(".")
            filters[f_name] = DashFilter(
                model_name=f_model,
                column_name=f_col,
                widget_type=f_data["widget_type"],
                default=f_data.get("default"),
            )

        return Dashboard(
            name=manifest_dash["name"],
            description=manifest_dash.get('description'),
            cards=cards,
            filters=filters,
            filters_order=meta.get('filters_order')
        )

    def _read_card(self, manifest_model) -> Card:
        path = manifest_model["original_file_path"]
        compile_path = self.target_dir.joinpath(
            "compiled", manifest_model["package_name"], path
        )
        with open(compile_path) as f:
            compile_sql = f.read()
        card_sql, filters = _replace_conditions_in_query(compile_sql)

        return Card(
            name=manifest_model["name"],
            path=path,
            compile_sql=compile_sql,
            card_sql=card_sql,
            filters=filters,
            description=manifest_model.get("description"),
        )

    def _read_model(
        self,
        manifest: Mapping,
        manifest_model: Mapping,
        group: Group,
        source: Optional[str] = None,
    ) -> Model:
        database = manifest_model["database"]
        schema = manifest_model["schema"]
        unique_id = manifest_model["unique_id"]

        relationships = self._read_relationships(manifest, group, unique_id)

        columns = [
            self._read_column(column, schema, relationships.get(column["name"]))
            for column in manifest_model.get("columns", {}).values()
        ]

        return Model(
            database=database,
            schema=schema,
            group=group,
            name=manifest_model["name"],
            alias=manifest_model.get(
                "alias", manifest_model.get("identifier", manifest_model["name"])
            ),
            description=manifest_model.get("description"),
            columns=columns,
            unique_id=unique_id,
            source=source,
            tags=manifest_model.get("tags", []),
            **self._scan_fields(
                manifest_model.get("meta", {}),
                fields=_MODEL_META_FIELDS,
                ns=_META_NS,
            ),
        )

    def _read_column(
        self,
        manifest_column: Mapping,
        schema: str,
        relationship: Optional[Mapping],
    ) -> Column:
        column = Column(
            name=manifest_column.get("name", ""),
            description=manifest_column.get("description"),
            **self._scan_fields(
                manifest_column.get("meta", {}),
                fields=_COLUMN_META_FIELDS,
                ns=_META_NS,
            ),
        )

        self._set_column_relationship(
            manifest_column=manifest_column,
            column=column,
            schema=schema,
            relationship=relationship,
        )

        return column

    def _read_relationships(
        self,
        manifest: Mapping,
        group: Group,
        unique_id: str,
    ) -> Mapping[str, Mapping[str, str]]:
        relationships = {}

        for child_id in manifest["child_map"][unique_id]:
            child = manifest.get(group, {}).get(child_id, {})
            child_name = child.get("alias", child.get("name"))

            if (
                child.get("resource_type") == "test"
                and child.get("test_metadata", {}).get("name") == "relationships"
            ):
                # To get the name of the foreign table, we could use child[test_metadata][kwargs][to], which
                # would return the ref() written in the test, but if the model has an alias, that's not enough.
                # Using child[depends_on][nodes] and excluding the current model is better.

                # Nodes contain at most two tables: referenced model and current model (optional).
                depends_on_nodes = list(child["depends_on"][group])

                # Relationships on disabled models mention them in refs but not depends_on,
                # which confuses the filtering logic that follows.
                depends_on_names = {n.split(".")[-1] for n in depends_on_nodes}
                mismatched_refs = []
                for ref in child["refs"]:
                    ref_name = ""
                    if isinstance(ref, dict):  # current manifest
                        ref_name = ref["name"]
                    elif isinstance(ref, list):  # old manifest
                        ref_name = ref[0]
                    if ref_name not in depends_on_names:
                        mismatched_refs.append(ref_name)

                if mismatched_refs:
                    _logger.debug(
                        "Mismatched refs %s with depends_on for relationship '%s', skipping",
                        mismatched_refs,
                        child_name,
                    )
                    continue

                if len(depends_on_nodes) > 2:
                    _logger.warning(
                        "Unexpected %d depends_on for relationship '%s' instead of <=2, skipping",
                        len(depends_on_nodes),
                        child_name,
                    )
                    continue

                # Skip the incoming relationship tests, in which the fk_target_table is the model currently being read.
                # Otherwise, the primary key of the current model would be (incorrectly) determined to be FK.
                if len(depends_on_nodes) == 2 and depends_on_nodes[1] != unique_id:
                    _logger.debug(
                        "Circular dependency '%s' for relationship '%s', skipping",
                        depends_on_nodes[1],
                        child_name,
                    )
                    continue

                # Remove the current model from the list, ensuring it works for self-referencing models.
                if len(depends_on_nodes) == 2 and unique_id in depends_on_nodes:
                    depends_on_nodes.remove(unique_id)

                if len(depends_on_nodes) != 1:
                    _logger.warning(
                        "Got %d dependencies for '%s' instead of 1, skipping",
                        len(depends_on_nodes),
                        unique_id,
                    )
                    continue

                depends_on_id = depends_on_nodes[0]

                fk_target_model = manifest[group].get(depends_on_id, {})
                fk_target_table = fk_target_model.get(
                    "alias",
                    fk_target_model.get("identifier", fk_target_model.get("name")),
                )
                if not fk_target_table:
                    _logger.debug("Cannot resolve dependency for '%s'", depends_on_id)
                    continue

                fk_target_schema = manifest[group][depends_on_id].get(
                    "schema", DEFAULT_SCHEMA
                )
                fk_target_table = f"{fk_target_schema}.{fk_target_table}"
                fk_target_field = child["test_metadata"]["kwargs"]["field"].strip('"')

                relationships[child["column_name"]] = {
                    "fk_target_table": fk_target_table,
                    "fk_target_field": fk_target_field,
                }

        return relationships

    def _set_column_relationship(
        self,
        manifest_column: Mapping,
        column: Column,
        schema: str,
        relationship: Optional[Mapping],
    ):
        """Sets primary key and foreign key target on a column from constraints, meta fields or provided test relationship."""

        fk_target_table = ""
        fk_target_field = ""

        # Precedence 1: Relationship test
        if relationship:
            fk_target_table = relationship["fk_target_table"]
            fk_target_field = relationship["fk_target_field"]

        # Precedence 2: Constraints
        for constraint in manifest_column.get("constraints", []):
            if constraint["type"] == "primary_key":
                if not column.semantic_type:
                    column.semantic_type = "type/PK"

            elif constraint["type"] == "foreign_key":
                constraint_expr = constraint.get("expression", "")
                constraint_fk = _CONSTRAINT_FK_PARSER.search(constraint_expr)
                if constraint_fk:
                    fk_target_table = constraint_fk.group("model")
                    fk_target_field = constraint_fk.group("column")
                else:
                    _logger.warning(
                        "Unparsable '%s' foreign key constraint: %s",
                        column.name,
                        constraint_expr,
                    )

        # Precedence 3: Meta fields
        meta = manifest_column.get("meta", {})
        fk_target_table = meta.get(f"{_META_NS}.fk_target_table", fk_target_table)
        fk_target_field = meta.get(f"{_META_NS}.fk_target_field", fk_target_field)

        if not fk_target_table or not fk_target_field:
            if fk_target_table or fk_target_table:
                _logger.warning(
                    "Foreign key requires table and field for column '%s'",
                    column.name,
                )
            return

        fk_target_table_path = fk_target_table.split(".")
        if len(fk_target_table_path) == 1 and schema:
            fk_target_table_path.insert(0, schema)

        column.semantic_type = "type/FK"
        column.fk_target_table = ".".join([x.strip('"') for x in fk_target_table_path])
        column.fk_target_field = fk_target_field.strip('"')
        _logger.debug(
            "Relation from '%s' to '%s.%s'",
            column.name,
            column.fk_target_table,
            column.fk_target_field,
        )

    @staticmethod
    def _scan_fields(t: Mapping, fields: Iterable[str], ns: str) -> Mapping:
        """Reads meta fields from a schem object.

        Args:
            t (Mapping): Target to scan for fields.
            fields (List): List of fields to accept.
            ns (str): Field namespace (separated by .).

        Returns:
            Mapping: Field values.
        """

        vals = {}
        for field in fields:
            if f"{ns}.{field}" in t:
                value = t[f"{ns}.{field}"]
                vals[field] = value if value is not None else NullValue
        return vals


class Group(str, Enum):
    nodes = "nodes"
    sources = "sources"


@dc.dataclass
class Column:
    name: str
    description: Optional[str] = None
    display_name: Optional[str] = None
    visibility_type: Optional[str] = None
    semantic_type: Optional[str] = None
    has_field_values: Optional[str] = None
    coercion_strategy: Optional[str] = None
    number_style: Optional[str] = None

    fk_target_table: Optional[str] = None
    fk_target_field: Optional[str] = None

    meta_fields: MutableMapping = dc.field(default_factory=dict)


@dc.dataclass
class Model:
    database: str
    schema: str
    group: Group

    name: str
    alias: str
    description: Optional[str] = None
    display_name: Optional[str] = None
    visibility_type: Optional[str] = None
    points_of_interest: Optional[str] = None
    caveats: Optional[str] = None

    unique_id: Optional[str] = None
    source: Optional[str] = None
    tags: Optional[Sequence[str]] = dc.field(default_factory=list)

    columns: Sequence[Column] = dc.field(default_factory=list)

    @property
    def ref(self) -> Optional[str]:
        if self.group == Group.nodes:
            return f"ref('{self.name}')"
        elif self.group == Group.sources:
            return f"source('{self.source}', '{self.name}')"
        return None

    def format_description(
        self,
        append_tags: bool = False,
        docs_url: Optional[str] = None,
    ) -> str:
        """Formats description from available information.

        Args:
            append_tags (bool, optional): True to include dbt model tags. Defaults to False.
            docs_url (Optional[str], optional): Provide docs base URL to include links. Defaults to None.

        Returns:
            str: Formatted description.
        """

        sections = []

        if self.description:
            sections.append(self.description)

        if append_tags and self.tags:
            sections.append(f"Tags: {', '.join(self.tags)}")

        if docs_url:
            sections.append(
                f"dbt docs: {docs_url.rstrip('/')}/#!/model/{self.unique_id}"
            )

        return "\n\n".join(sections)


@dc.dataclass
class Card:
    name: str
    path: str
    compile_sql: str
    card_sql: Optional[str] = None
    card_id: Optional[int] = None
    description: Optional[str] = None
    tags: list[str] = dc.field(default_factory=list)
    filters: set[str] = dc.field(default_factory=set)


@dc.dataclass
class DashFilter:
    model_name: str
    column_name: str
    widget_type: str
    id: Optional[str] = None
    default: Optional[str] = None
    db_id: Optional[str] = None
    column_id: Optional[int] = None
    column_effective_type: Optional[str] = None
    column_base_type: Optional[str] = None


@dc.dataclass
class Dashboard:
    name: str
    description: Optional[str] = None
    cards: Sequence[Card] = dc.field(default_factory=list)
    filters: dict[str, DashFilter] = dc.field(default_factory=dict)
    filters_order: Optional[list[str]] = None
