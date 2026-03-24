from pathlib import Path

from core.config.loader import get_agent_config, load_config


def load_system_prompt(industry: str) -> str:
    config = load_config(industry)
    prompt_path = Path(get_agent_config(config)["system_prompt_file"])
    return prompt_path.read_text(encoding="utf-8")
