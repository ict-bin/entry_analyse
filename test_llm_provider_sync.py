from app.service.llm_provider_sync import build_models_json


def test_build_models_json_adds_glm_compatible_thinking_level_map():
    payload = build_models_json(
        [
            {
                "enabled": True,
                "provider_key": "icsl_litellm",
                "provider_type": "openai",
                "api_base": "http://api.ai.icsl.huawei.com/v1",
                "api_key": "sk-test",
                "model": "zai-org/GLM-5.1-180K",
            }
        ]
    )

    model = payload["providers"]["icsl_litellm"]["models"][0]
    assert model["reasoning"] is False
    assert model["thinkingLevelMap"]["disabled"] == "disabled"
