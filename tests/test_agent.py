from core.agent.personas import load_system_prompt


def test_system_prompts_load_for_all_industries():
    for industry in ["mining", "energy", "water", "automotive", "semiconductor"]:
        prompt = load_system_prompt(industry)
        assert isinstance(prompt, str)
        assert len(prompt.strip()) > 20
