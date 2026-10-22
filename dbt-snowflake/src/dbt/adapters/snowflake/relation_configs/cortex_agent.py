from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dbt_common.exceptions import CompilationError

from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase


@dataclass(frozen=True)
class SnowflakeCortexAgentConfig(SnowflakeRelationConfigBase):
    profile: Dict[str, str] = field(default_factory=dict)
    comment: Optional[str] = None
    secure: Optional[bool] = None
    copy_grants: bool = False

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config = relation_config.config
        meta = config.get("meta", {}) if config else {}
        meta = meta or {}
        profile = config.get("profile") if config else None
        if profile is None:
            profile = {"display_name": meta.get("display_name") or relation_config.identifier}
        comment = config.get("comment") if config else None
        if comment is None:
            comment = meta.get("comment")
        if comment is None:
            comment = relation_config.description or None
        return {
            "profile": profile,
            "comment": comment,
            "secure": config.get("secure") if config else None,
            "copy_grants": config.get("copy_grants") if config else None,
        }

    def __post_init__(self) -> None:
        for name in ("secure", "copy_grants"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, bool):
                raise CompilationError(f"cortex_agent `{name}` must be a boolean.")
        if self.comment is not None and not isinstance(self.comment, str):
            raise CompilationError("cortex_agent `comment` must be a string.")
        if not isinstance(self.profile, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in self.profile.items()
        ):
            raise CompilationError("cortex_agent `profile` must be an object with string values.")
