import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def validate_and_fix_bog_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates BOG staging config and automatically fixes invalid 'days_range' values.
    Replaces invalid ranges with safe defaults:
      Current: 0-30
      Olem: 31-60
      Substandard: 61-90
      Doubtful: 91-180
      Loss: 181+
    """
    # Default safe ranges
    default_ranges = {
        "current": "0-30",
        "olem": "31-60",
        "substandard": "61-90",
        "doubtful": "91-180",
        "loss": "181+"
    }

    fixed_config = {}

    for stage, default_range in default_ranges.items():
        stage_data = config.get(stage, {})
        days_range = stage_data.get("days_range", "")

        try:
            # Try parsing using existing parse_days_range
            from app.utils.staging import parse_days_range
            parse_days_range(days_range)
            fixed_config[stage] = {"days_range": days_range}
        except Exception:
            logger.warning(f"Invalid days_range '{days_range}' for stage '{stage}'. "
                           f"Replacing with default '{default_range}'.")
            fixed_config[stage] = {"days_range": default_range}

    return fixed_config
