"""DeepSeek 專用 LLM client。

問題：DeepSeek 不支援 json_schema response_format（只支援 json_object），而在
json_object 模式下，DeepSeek 有時會照抄 schema 形狀、把真正的內容多包一層
"properties"，例如回傳 {"properties": {"duplicate_facts": [...], ...}}，
導致 Graphiti 的 `SomeModel(**llm_response)` 解析失敗。

解法：覆寫 _generate_response，偵測到「目標 model 的必填欄位不在頂層、卻在
result['properties'] 裡」時，把那層攤平。其餘情況原樣返回，不影響正常回應。
"""
from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient


def _unwrap_properties(result, response_model):
    if not isinstance(result, dict):
        return result
    inner = result.get('properties')
    if not isinstance(inner, dict):
        return result
    if response_model is not None:
        required = set(getattr(response_model, 'model_fields', {}).keys())
        # 僅當必填欄位不在頂層、但在 inner 裡時才解包
        if required and not (required & result.keys()) and (required & inner.keys()):
            return inner
        return result
    # 沒給 model 時：若 properties 是唯一實質鍵，視為被多包一層
    if set(result.keys()) <= {'properties', 'type', 'required', 'title'}:
        return inner
    return result


class DeepSeekClient(OpenAIGenericClient):
    async def _generate_response(self, messages, response_model=None, *args, **kwargs):
        result = await super()._generate_response(messages, response_model, *args, **kwargs)
        return _unwrap_properties(result, response_model)
