import os
import asyncio
import aiohttp
import unittest
import batch


class Test(unittest.TestCase):
    def setUp(self):
        session = aiohttp.ClientSession(
            raise_for_status=True,
            timeout=aiohttp.ClientTimeout(total=60))
        self.client = batch.aioclient.BatchClient(session, url=os.environ.get('BATCH_URL'))

    def tearDown(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.client.close())

    def test_job(self):
        async def f():
            b = self.client.create_batch()
            j = b.create_job('alpine', ['echo', 'test'])
            await b.submit()
            status = await j.wait()
            self.assertTrue('attributes' not in status)
            self.assertEqual(status['state'], 'Success')
            self.assertEqual(status['exit_code']['main'], 0)

            self.assertEqual(await j.log(), {'main': 'test\n'})

            self.assertTrue(await j.is_complete())

        loop = asyncio.get_event_loop()
        loop.run_until_complete(f())
