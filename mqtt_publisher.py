import time
import socket
from paho.mqtt import client as mqtt_client


class MQTTPublisher:
    def __init__(self, broker_host: str = "localhost", broker_port: int = 1883, client_id: str = None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        # generate client id if none provided
        self.client_id = client_id or f"voice-publisher-{int(time.time())}-{socket.gethostname()}"
        self._client = mqtt_client.Client(self.client_id)
        self._connected = False

        # optional callbacks
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

        try:
            self._client.connect(self.broker_host, self.broker_port)
            self._client.loop_start()
            # small wait for connect
            time.sleep(0.2)
        except Exception:
            # leave the client in a disconnected state; publish will raise if needed
            pass

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected = True

    def _on_disconnect(self, client, userdata, rc):
        self._connected = False

    def publish(self, topic: str, payload: str, qos: int = 1, retain: bool = False):
        if not self._connected:
            # try a reconnect
            try:
                self._client.reconnect()
            except Exception as e:
                raise ConnectionError(f"MQTT broker not reachable: {e}")

        result = self._client.publish(topic, payload, qos=qos, retain=retain)
        status = result[0]
        if status != 0:
            raise RuntimeError(f"Failed to send message to topic {topic} (status {status})")
