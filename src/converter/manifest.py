"""
Migration manifest generator.

Produces a YAML artefact summarising the results of a SAS to Databricks
conversion run — overall confidence, per-block details, and items requiring
manual review. Used as an audit trail by the migration team.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

import yaml


@dataclass
class ManifestBlock:
    block_id: int
    sas_snippet: str
    method: str
    confidence: float
    review_required: bool
    notes: List[str]
    warnings: List[str]


@dataclass
class MigrationManifest:
    source_label: str
    generated_at: str
    platform: str
    config_applied: bool
    blocks: List[ManifestBlock] = field(default_factory=list)

    @property
    def total_blocks(self) -> int:
        return len(self.blocks)

    @property
    def rule_engine_hits(self) -> int:
        return sum(1 for b in self.blocks if b.method == "rule_based")

    @property
    def llm_fallback(self) -> int:
        return sum(1 for b in self.blocks if b.method == "llm")

    @property
    def review_required_count(self) -> int:
        return sum(1 for b in self.blocks if b.review_required)

    @property
    def overall_confidence(self) -> float:
        if not self.blocks:
            return 0.0
        return round(sum(b.confidence for b in self.blocks) / len(self.blocks), 2)


def generate_manifest(
    results: list,
    source_label: str = "sas_input",
    platform: str = "community",
    config_applied: bool = False,
) -> str:
    """
    Generate a YAML migration manifest from a list of ConversionResult objects.

    Parameters
    ----------
    results : list of ConversionResult
    source_label : str
        Filename or label identifying the source SAS script.
    platform : str
        "community" or "enterprise".
    config_applied : bool
        Whether a MigrationConfig was used for this conversion.

    Returns
    -------
    str
        YAML string suitable for download or display.
    """
    blocks = []
    for i, r in enumerate(results, 1):
        snippet = " ".join(r.original.strip().split())[:120]
        blocks.append(ManifestBlock(
            block_id=i,
            sas_snippet=snippet,
            method=r.method,
            confidence=r.confidence,
            review_required=r.review_required,
            notes=r.notes,
            warnings=r.warnings,
        ))

    manifest = MigrationManifest(
        source_label=source_label,
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        platform=platform,
        config_applied=config_applied,
        blocks=blocks,
    )

    doc = {
        "migration_manifest": {
            "source": manifest.source_label,
            "generated_at": manifest.generated_at,
            "platform": manifest.platform,
            "config_applied": manifest.config_applied,
            "summary": {
                "total_blocks": manifest.total_blocks,
                "rule_engine_hits": manifest.rule_engine_hits,
                "llm_fallback": manifest.llm_fallback,
                "review_required": manifest.review_required_count,
                "overall_confidence": manifest.overall_confidence,
            },
            "blocks": [
                {
                    "block_id": b.block_id,
                    "sas_snippet": b.sas_snippet,
                    "method": b.method,
                    "confidence": b.confidence,
                    "review_required": b.review_required,
                    "notes": b.notes if b.notes else [],
                    "warnings": b.warnings if b.warnings else [],
                }
                for b in manifest.blocks
            ],
        }
    }

    return yaml.dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)
