import time

from tornado import gen
from tornado.testing import AsyncTestCase
from toredis.client import SentinelClient

class TestSentinelClient(AsyncTestCase):
    """ Test the client """

    def setUp(self):
        super(TestSentinelClient, self).setUp()
        self.client = SentinelClient(io_loop=self.io_loop)

    def connect(self, callback=None, client=None, on_disconnect=None):
        if callback is None:
            callback = self.stop
        if client is None:
            client = self.client

        client._disconnect_callback = on_disconnect

        client.connect(
            sentinels=['10.11.165.99:26379', '10.11.165.255:26383', '10.11.165.8:26379', '10.11.165.18:26379', '10.11.165.17:26379'],
            master_name="redis-oniasqa-new",
            callback=callback
        )

    def test_connect(self):
        result = {}

        def callback():
            result["connected"] = True
            self.assertEqual(self.client.connection_status, "CONNECTED")
            self.stop()

        self.connect(callback)
        self.wait()  # blocks

        self.assertTrue("connected" in result)

    @gen.engine
    def test_set_command(self):
        result = {}

        def set_callback(response):
            result["set"] = response
            self.stop()

        self.connect(self.stop)
        self.wait()

        self.client.set("foo", "bar", callback=set_callback)
        self.wait()
        #blocks
        self.assertTrue("set" in result)
        self.assertEqual(result["set"], b"OK")

        value = yield gen.Task(self.client.get, "foo")
        self.assertEqual("bar", value)

    @gen.engine
    def test_get_command(self):
        result = None

        def get_callback(response):
            result = response
            self.stop()

        time_string = "%s" % time.time()

        self.connect(self.stop)
        self.wait()

        yield gen.Task(self.client.set, "foo", time_string)

        self.client.get("foo", callback=get_callback)
        self.wait()
        #blocks

        self.assertTrue(result is not None, 'result is %s' % result)
        self.assertEqual(time_string, result)

    def test_sub_command(self):
        client = SentinelClient(io_loop=self.io_loop)
        result = {"message_count": 0}
        conn = SentinelClient(io_loop=self.io_loop)

        self.connect(self.stop, client)
        self.connect(self.stop, conn)

        response = yield gen.Task(client.subscribe, "foobar")
        if response[0] == "subscribe":
            result["sub"] = response
            yield gen.Task(conn.publish, "foobar", "new message!")
        elif response[0] == "message":
            result["message_count"] += 1
            if result["message_count"] < 100:
                count = result["message_count"]
                value = yield gen.Task(conn.publish,
                                       "foobar", "new message %s!" % count)
            result["message"] = response[2]

        self.assertTrue("sub" in result)
        self.assertTrue("message" in result)
        self.assertTrue(result["message"], "new message 99!")

    def test_pub_command(self):
        result = {}

        def pub_callback(response):
            result["pub"] = response
            self.stop()

        self.connect(self.stop)
        self.wait()
        self.client.publish("foobar", "message", callback=pub_callback)
        self.wait()
        # blocks
        self.assertTrue("pub" in result)
        self.assertEqual(result["pub"], 0)  # no subscribers yet

    def test_blpop(self):
        result = {}

        def rpush_callback(response):
            result["push"] = response

            def blpop_callback(response):
                result["pop"] = response
                self.stop()

            self.client.blpop("test", 0, blpop_callback)

        self.connect()
        self.wait()

        self.client.rpush("test", "dummy", rpush_callback)
        self.wait()

        self.assertEqual(result["pop"], [b"test", b"dummy"])

    def test_disconnect(self):
        self.connect()
        self.wait()

        self.client.close()
        with self.assertRaises(IOError):
            self.client._stream.read_bytes(1024, lambda x: x)

    def test_on_disconnect(self):
        result = {}

        def on_disconnect(*args, **kw):
            result['disconnected'] = True

        self.connect(on_disconnect=on_disconnect)
        self.wait()

        self.assertTrue(result['disconnected'])
