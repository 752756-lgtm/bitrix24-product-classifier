import json
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from http.server import ThreadingHTTPServer

from product_picker.core import (AI, Bitrix, Config, PickerError, Pipeline, RetryLater,
                                 identifier, plain, product_view, render)
from product_picker.server import Queue, Worker, decode, handler


def config():
    return Config('https://100kwatt.bitrix24.ru/rest/1/example/', 'https://100kwatt.ru/api/2.0',
                  'example@example.test', 'not-a-key', 'not-a-key', 'test-model', 'x' * 40)


def product(pid=7):
    return {'product_id': str(pid), 'product': 'TEST X-12 привод', 'status': 'A',
            'product_code': 'TEST-7', 'price': '0', 'amount': '1001',
            'product_features': {'1': {'description': 'Наличие', 'variant': 'Требует уточнения'}}}


class FakeBitrix:
    def __init__(self):
        self.comments = []
        self.digest = 'initial'
        self.reads = 0
        self.write_error = False
        self.email_error = False

    def email(self, deal, activity=0):
        if self.email_error:
            raise RetryLater('email_not_linked_yet')
        self.reads += 1
        return {'id': 11, 'text': 'Нужен TEST X-12 с комплектом кассет.',
                'digest': self.digest, 'has_attachments': True}

    def comment_exists(self, deal, marker):
        return any(marker in x for x in self.comments)

    def add_comment(self, deal, text):
        self.comments.append(text)
        if self.write_error:
            raise RetryLater('upstream_unavailable')
        return 1


class FakeAI:
    def extract(self, text):
        return [{'quote': 'TEST X-12', 'request': 'TEST X-12 с комплектом кассет',
                 'quantity': 'не указано', 'queries': ['TEST X-12']}]

    def rank(self, items, products):
        return [{'index': 0, 'questions': ['Уточнить состав кассет'], 'candidates': [
            {'product_id': 7, 'assessment': 'Привод без подтверждённого комплекта',
             'mismatches': ['Комплект кассет не подтверждён']}]}]


class FakeCatalog:
    def search(self, query):
        return [product()], False

    def product(self, pid):
        return product_view(product(pid))


class CoreTests(unittest.TestCase):
    def test_ids_reject_bools_and_injection(self):
        for value in [True, 0, -1, '3&secret=a', '1/../2', 1.2]:
            with self.assertRaises(PickerError):
                identifier(value)

    def test_html_does_not_fetch_images_or_include_scripts(self):
        self.assertEqual(plain('<head><style>secret</style></head><p>TEST</p><img src="https://bad.test"><script>ignore</script>'), 'TEST')

    def test_zero_price_and_synthetic_stock_not_claimed_as_available(self):
        p = product_view(product())
        self.assertEqual(p['price'], 'не подтверждена')
        self.assertEqual(p['availability'], 'Требует уточнения')
        self.assertNotIn('amount', p)

    def test_foreign_product_url_not_used(self):
        raw = product()
        raw['url'] = 'https://bad.test/instructions'
        self.assertTrue(product_view(raw)['url'].startswith('https://100kwatt.ru/'))

    def test_configuration_rejects_foreign_host_and_short_secret(self):
        env = {'BITRIX_WEBHOOK_URL': config().bitrix_url, 'CSCART_API_EMAIL': 'u',
               'CSCART_API_KEY': 'k', 'OPENAI_API_KEY': 'a', 'OPENAI_MODEL': 'm',
               'PICKER_WEBHOOK_SECRET': 'x' * 40}
        with patch.dict('os.environ', env, clear=True):
            self.assertFalse(Config.from_env().write_enabled)
        for extra in [{'CSCART_API_URL': 'https://bad.test/api'}, {'PICKER_WEBHOOK_SECRET': 'short'},
                      {'BITRIX_WEBHOOK_URL': 'http://100kwatt.bitrix24.ru/rest/1/a/'}]:
            with patch.dict('os.environ', {**env, **extra}, clear=True), self.assertRaises(PickerError):
                Config.from_env()

    def test_extraction_requires_actual_quote(self):
        ai = AI(config())
        ai.structured = lambda *args: {'items': [{'quote': 'not in message', 'request': 'q', 'quantity': '1', 'queries': ['q']}]}
        with self.assertRaisesRegex(PickerError, 'ungrounded'):
            ai.extract('actual message')

    def test_rank_cannot_invent_products_or_cross_items(self):
        ai = AI(config())
        ai.structured = lambda *args: {'items': [{'index': 0, 'questions': [], 'candidates': [
            {'product_id': 999, 'assessment': 'exact', 'mismatches': []}]}]}
        with self.assertRaisesRegex(PickerError, 'unknown_product'):
            ai.rank([{}], [[product_view(product())]])

    def test_changed_source_blocks_output(self):
        bitrix = FakeBitrix()
        catalog = FakeCatalog()
        def changed(pid):
            bitrix.digest = 'changed'
            return product_view(product(pid))
        catalog.product = changed
        with self.assertRaisesRegex(PickerError, 'email_changed'):
            Pipeline(bitrix, catalog, FakeAI()).prepare(2)

    def test_report_preserves_kit_warning_and_review_requirement(self):
        result = Pipeline(FakeBitrix(), FakeCatalog(), FakeAI()).prepare(2)
        text = render(result)
        self.assertIn('требуется проверка менеджером', text)
        self.assertIn('Комплект кассет не подтверждён', text)
        self.assertIn('Вложения', text)
        self.assertNotIn('1001', text)

    def test_malformed_upstream_does_not_escape_as_attribute_error(self):
        with patch('product_picker.core.request_json', return_value=[]), self.assertRaises(PickerError):
            Bitrix(config()).raw('crm.deal.get', {})

    def test_full_binding_page_requires_another_read(self):
        bitrix = Bitrix(config())
        calls = []
        def raw(method, params):
            calls.append(params['start'])
            return {'result': [{'entityId': i + 1, 'entityTypeId': 3} for i in range(50)] if params['start'] == 0 else []}
        bitrix.raw = raw
        self.assertEqual(len(bitrix.rows('crm.activity.binding.list', {})), 50)
        self.assertEqual(calls, [0, 50])

    def test_ambiguous_binding_rejected(self):
        bitrix = Bitrix(config())
        bitrix.deal = lambda x: {}
        bitrix.call = lambda *args: {'ID': '11', 'TYPE_ID': '4', 'DIRECTION': '1', 'DESCRIPTION': 'Hello'}
        bitrix.rows = lambda *args, **kwargs: [{'entityId': 2, 'entityTypeId': 2}, {'entityId': 3, 'entityTypeId': 2}]
        with self.assertRaisesRegex(PickerError, 'ambiguous'):
            bitrix.email(2, 11)

    def test_rotating_attachment_urls_do_not_change_source_guard(self):
        bitrix = Bitrix(config())
        bitrix.deal = lambda x: {}
        activity = {'ID': '11', 'TYPE_ID': '4', 'DIRECTION': '1', 'DESCRIPTION': 'Hello',
                    'FILES': [{'id': 4, 'url': 'https://example.test/first-signature'}]}
        bitrix.call = lambda *args: activity
        bitrix.rows = lambda *args, **kwargs: [{'entityId': 2, 'entityTypeId': 2}]
        first = bitrix.email(2, 11)['digest']
        activity['FILES'][0]['url'] = 'https://example.test/second-signature'
        self.assertEqual(first, bitrix.email(2, 11)['digest'])


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.queue = Queue(str(Path(self.tmp.name) / 'queue.sqlite3'))
        self.bitrix = FakeBitrix()
        self.pipeline = Pipeline(self.bitrix, FakeCatalog(), FakeAI())

    def tearDown(self):
        self.tmp.cleanup()

    def test_duplicate_webhook_is_one_job(self):
        a = self.queue.enqueue(2, 0, False)
        b = self.queue.enqueue(2, 0, False)
        self.assertEqual(a['id'], b['id'])

    def test_dry_run_never_writes_and_does_not_block_later_write(self):
        dry = self.queue.enqueue(2, 0, False)
        Worker(config(), self.queue, self.pipeline).run_once()
        self.assertEqual(self.queue.get(dry['id'])['state'], 'dry_run_done')
        self.assertEqual(self.bitrix.comments, [])
        write = self.queue.enqueue(2, 0, True)
        Worker(replace(config(), write_enabled=True), self.queue, self.pipeline).run_once()
        self.assertEqual(self.queue.get(write['id'])['state'], 'done')
        self.assertEqual(len(self.bitrix.comments), 1)

    def test_write_timeout_never_reposts_comment(self):
        self.bitrix.write_error = True
        job = self.queue.enqueue(2, 0, True)
        worker = Worker(replace(config(), write_enabled=True), self.queue, self.pipeline)
        worker.run_once()
        self.assertEqual(self.queue.get(job['id'])['state'], 'uncertain')
        self.queue.recover()
        worker.run_once()
        self.assertEqual(len(self.bitrix.comments), 1)

    def test_restart_recovery_preserves_uncertainty_boundary(self):
        job = self.queue.enqueue(2, 0, True)
        self.queue.finish(job['id'], 'writing')
        self.queue.recover()
        self.assertEqual(self.queue.get(job['id'])['state'], 'uncertain')
        self.assertIsNone(self.queue.claim())

    def test_missing_email_is_delayed(self):
        self.bitrix.email_error = True
        job = self.queue.enqueue(2, 0, False)
        Worker(config(), self.queue, self.pipeline).run_once()
        self.assertEqual(self.queue.get(job['id'])['state'], 'queued')
        self.assertIsNone(self.queue.claim())

    def test_existing_comment_is_not_duplicated(self):
        result = self.pipeline.prepare(2)
        self.bitrix.comments = [render(result)]
        job = self.queue.enqueue(2, 0, True)
        Worker(replace(config(), write_enabled=True), self.queue, self.pipeline).run_once()
        self.assertEqual(self.queue.get(job['id'])['state'], 'done')
        self.assertEqual(len(self.bitrix.comments), 1)

    def test_new_request_id_cannot_duplicate_same_email(self):
        worker = Worker(replace(config(), write_enabled=True), self.queue, self.pipeline)
        self.queue.enqueue(2, 0, True)
        worker.run_once()
        second = self.queue.enqueue(2, 11, True)
        worker.run_once()
        self.assertEqual(self.queue.get(second['id'])['state'], 'duplicate')
        self.assertEqual(len(self.bitrix.comments), 1)

    def test_file_private(self):
        self.assertEqual(Path(self.queue.path).stat().st_mode & 0o777, 0o600)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.queue = Queue(str(Path(self.tmp.name) / 'queue.sqlite3'))
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), handler(config(), self.queue, threading.current_thread()))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.tmp.cleanup()

    def post(self, data, path='/bitrix/email-pick'):
        request = Request(self.url + path, json.dumps(data).encode(), {'Content-Type': 'application/json'})
        try:
            with urlopen(request, timeout=2) as r:
                return r.status, json.load(r)
        except HTTPError as e:
            return e.code, json.load(e)

    def test_missing_secret_rejected(self):
        self.assertEqual(self.post({'deal_id': 2})[0], 401)

    def test_body_secret_accepted_without_querystring(self):
        status, result = self.post({'deal_id': 2, 'secret': config().secret})
        self.assertEqual(status, 202)
        self.assertFalse(self.queue.get(result['id'])['write_mode'])

    def test_client_cannot_enable_writing(self):
        self.assertEqual(self.post({'deal_id': 2, 'secret': config().secret, 'write_enabled': True})[0], 400)

    def test_secret_in_url_not_supported(self):
        self.assertEqual(self.post({'deal_id': 2}, '/bitrix/email-pick?secret=x')[0], 404)

    def test_form_supported_and_duplicate_fields_rejected(self):
        self.assertEqual(decode(b'deal_id=2&secret=x', 'application/x-www-form-urlencoded')['deal_id'], '2')
        with self.assertRaises(PickerError):
            decode(b'deal_id=2&deal_id=3', 'application/x-www-form-urlencoded')


if __name__ == '__main__':
    unittest.main()
