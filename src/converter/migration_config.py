"""
Migration configuration for enterprise SAS to Databricks migrations.

Provides source-to-target mappings, macro variable values, and platform
settings. When passed to convert(), the converter resolves library
references and macro variables to produce Unity Catalog-ready output.
"""

from dataclasses import dataclass, field
from typing import Dict

import yaml


@dataclass
class MigrationConfig:
    """
    Configuration for a SAS to Databricks migration.

    Attributes
    ----------
    library_mappings : dict
        Maps SAS LIBNAME identifiers to Databricks catalog.schema.
        Example: {"risklib": "trading.bronze", "outlib": "trading.silver"}
    dataset_mappings : dict
        Maps libname.dataset to a fully qualified Databricks table name.
        Takes precedence over library_mappings for specific tables.
        Example: {"risklib.sp500_prices": "trading.bronze.bronze_sp500"}
    macro_vars : dict
        Maps SAS macro variable names (without &) to resolved string values.
        Example: {"start_date": "2010-01-01", "report_year": "2024"}
    target_catalog : str
        Default Databricks catalog for tables with no explicit mapping.
    target_schema : str
        Default Databricks schema for tables with no explicit mapping.
    unity_catalog : bool
        When True, emit catalog.schema.table references in converted code.
    platform : str
        "community" or "enterprise". Enterprise enables Unity Catalog paths
        and manifest generation.
    """

    library_mappings: Dict[str, str] = field(default_factory=dict)
    dataset_mappings: Dict[str, str] = field(default_factory=dict)
    macro_vars: Dict[str, str] = field(default_factory=dict)
    target_catalog: str = ""
    target_schema: str = ""
    unity_catalog: bool = False
    platform: str = "community"

    def resolve_table(self, sas_ref: str) -> str:
        """
        Resolve a SAS dataset reference to a Databricks table name.

        Checks dataset_mappings first, then derives from library_mappings.
        Falls back to the original reference if no mapping exists.
        """
        sas_ref_lower = sas_ref.lower()

        if sas_ref_lower in {k.lower() for k in self.dataset_mappings}:
            for k, v in self.dataset_mappings.items():
                if k.lower() == sas_ref_lower:
                    return v

        if "." in sas_ref:
            lib, ds = sas_ref.split(".", 1)
            lib_lower = lib.lower()
            for k, v in self.library_mappings.items():
                if k.lower() == lib_lower:
                    if self.unity_catalog:
                        return f"{v}.{ds.lower()}"
                    else:
                        return ds.lower()

        return sas_ref


def load_config(path: str) -> MigrationConfig:
    """Load a MigrationConfig from a YAML file path."""
    with open(path, "r") as f:
        return load_config_from_dict(yaml.safe_load(f))


def load_config_from_dict(raw: dict) -> MigrationConfig:
    """Load a MigrationConfig from a parsed YAML dict."""
    source = raw.get("source", {})
    target = raw.get("target", {})
    return MigrationConfig(
        library_mappings=source.get("library_mappings", {}),
        dataset_mappings=source.get("dataset_mappings", {}),
        macro_vars=source.get("macro_vars", {}),
        target_catalog=target.get("catalog", ""),
        target_schema=target.get("default_schema", ""),
        unity_catalog=target.get("unity_catalog", False),
        platform=target.get("platform", "enterprise"),
    )
