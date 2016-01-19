# stdlib
from collections import defaultdict
import random
import json

# project
from checks import AgentCheck

# 3rd party
from kafka.client import KafkaClient
from kafka.common import OffsetRequest
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

class KafkaCheck(AgentCheck):

    SOURCE_TYPE_NAME = 'kafka'

    def check(self, instance):
	znode = 'znode'
        consumer_groups = self.read_config(instance, 'consumer_groups')
#                                           cast=self._validate_consumer_groups)
        zk_connect_str = self.read_config(instance, 'zk_connect_str')
        kafka_host_ports = self.read_config(instance, 'kafka_connect_str')
        # Construct the Zookeeper path pattern
        zk_prefix = instance.get('zk_prefix', '')
        zk_path_tmpl = zk_prefix + '/consumers/%s/offsets/%s/%s'

        # Connect to Zookeeper
        zk_conn = KazooClient(zk_connect_str)
        zk_conn.start()
        try:
            # Query Zookeeper for consumer offsets
            consumer_offsets = {}
            topics = defaultdict(set)
            for consumer_group in consumer_groups.iteritems():
		consumer = consumer_group[0]
		if znode in consumer_group[1]:
		    zn = consumer_group[1]['znode']
		else:
		    zn = 'consumers'
		for k,v in consumer_group[1].iteritems():
		    if k != znode:
			topics[k].update(set(v))

		for topic in topics.iteritems():
		    if topic[0] in consumer_group[1]:
			if zn == 'consumers':
				for partition in topic[1]:
				    zk_path = zk_path_tmpl % (consumer, topic[0], partition)
				    try:
					consumer_offset = int(zk_conn.get(zk_path)[0])
					key = (consumer, topic[0], partition)
					consumer_offsets[key] = consumer_offset
				    except NoNodeError:
					self.log.warn('No zookeeper node at %s' % zk_path)
				    except Exception:
					self.log.exception('Could not read consumer offset from %s' % zk_path)
			else:
				for partition in topic[1]:
				    part = 'partition_%s' % partition
				    zk_path = '/%s/%s/%s' % (zn, consumer, part)
				    try:
					consumer_offset_json = json.loads(zk_conn.get(zk_path)[0])
					consumer_offset = consumer_offset_json['offset']
					key = (consumer, topic[0], partition)
					consumer_offsets[key] = consumer_offset
                                    except NoNodeError:
                                        self.log.warn('No zookeeper node at %s' % zk_path)
                                    except Exception:
                                        self.log.exception('Could not read consumer offset from %s' % zk_path)

        finally:
            try:
                zk_conn.stop()
                zk_conn.close()
            except Exception:
                self.log.exception('Error cleaning up Zookeeper connection')

        # Connect to Kafka
        kafka_conn = KafkaClient(kafka_host_ports)

        try:
            # Query Kafka for the broker offsets
            broker_offsets = {}
            for topic, partitions in topics.items():
                offset_responses = kafka_conn.send_offset_request([
                    OffsetRequest(topic, p, -1, 1) for p in partitions])
                for resp in offset_responses:
                    broker_offsets[(resp.topic, resp.partition)] = resp.offsets[0]

        finally:
            try:
                kafka_conn.close()
            except Exception:
                self.log.exception('Error cleaning up Kafka connection')

        # Report the broker data
        for (topic, partition), broker_offset in broker_offsets.items():
            broker_tags = ['topic:%s' % topic, 'partition:%s' % partition]
            broker_offset = broker_offsets.get((topic, partition))
            self.gauge('kafka.broker_offset', broker_offset, tags=broker_tags)

        # Report the consumer
        for (consumer_group, topic, partition), consumer_offset in consumer_offsets.items():

            # Get the broker offset
            broker_offset = broker_offsets.get((topic, partition))
            # Report the consumer offset and lag
            tags = ['topic:%s' % topic, 'partition:%s' % partition,
                    'consumer_group:%s' % consumer_group]
            self.gauge('kafka.consumer_offset', consumer_offset, tags=tags)
            self.gauge('kafka.consumer_lag', broker_offset - consumer_offset,
                       tags=tags)

    # Private config validation/marshalling functions

    def _validate_consumer_groups(self, val):
        try:
            consumer_group, topic_partitions, consumer_path = val.items()[0]
            assert isinstance(consumer_group, (str, unicode))
            topic, partitions = topic_partitions.items()[0]
            assert isinstance(topic, (str, unicode))
            assert isinstance(partitions, (list, tuple))
	    assert isinstance(consumer_path, (str, unicode))
            return val
        except Exception, e:
            self.log.exception(e)
            raise Exception('''The `consumer_groups` value must be a mapping of mappings, like this:
consumer_groups:
  myconsumer0: # consumer group name
    mytopic0: [0, 1] # topic: list of partitions
  myconsumer1:
    mytopic0: [0, 1, 2]
    mytopic1: [10, 12]
''')
