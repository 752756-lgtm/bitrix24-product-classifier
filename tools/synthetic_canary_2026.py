"""Synthetic-only response-shape probe. No Bitrix credential or CRM input."""
import json
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from classifier.activity_prepare import OpenAIActivityClassifier, CodedPreparationError, TransientPreparationError
from classifier.ai import _output_text
from classifier.http import post_json


def inspect_response(url, payload, headers, timeout):
    response = post_json(url, payload, headers, timeout)
    expected = OpenAIActivityClassifier._canary_value()
    safe = {"envelope_dict": isinstance(response, dict)}
    if isinstance(response, dict):
        status = response.get('status')
        safe['status'] = status if status in ('completed', 'incomplete', 'failed', 'in_progress', 'queued') else 'other'
        safe['output_list'] = isinstance(response.get('output'), list)
        safe['output_count'] = len(response['output']) if safe['output_list'] else 0
        try:
            text = _output_text(response)
            safe['text_length'] = len(text)
            value = json.loads(text)
            safe['json_object'] = isinstance(value, dict)
            if isinstance(value, dict):
                safe['exact_keys'] = set(value) == set(expected)
                safe['matching_fields'] = {k: value.get(k) == v for k, v in expected.items()}
        except (ValueError, TypeError, KeyError, RuntimeError):
            safe['parse_failed'] = True
    print(json.dumps(safe, sort_keys=True), flush=True)
    return response


if __name__ == '__main__':
    try:
        OpenAIActivityClassifier(os.environ['OPENAI_API_KEY'], 'gpt-5.6-luna', requester=inspect_response).canary()
        print('SYNTHETIC_CANARY_OK')
    except (CodedPreparationError, TransientPreparationError) as exc:
        print(json.dumps({'failure_code': exc.failure_code}))
        raise SystemExit(1)
