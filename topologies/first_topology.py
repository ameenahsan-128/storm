from streamparse import Grouping, Topology
from src.spouts.Kafkaspout import KafkaSpout
from src.bolts.First_bolt import Emit
from src.bolts.spark_bolt import SparkExecutionBolt
from src.bolts.hdfs_status_bolt import HDFS

class MyTopology(Topology):
    kafka_spout = KafkaSpout.spec()
    first_bolt = Emit.spec(inputs={kafka_spout: Grouping.fields('message')})
    spark_bolt = SparkExecutionBolt.spec(inputs={first_bolt: Grouping.fields('output')})
    hdfs_status_bolt = HDFS.spec(inputs={spark_bolt: Grouping.fields('status')})

if __name__ == '__main__':
    MyTopology().submit('my_topology')
