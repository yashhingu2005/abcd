

> [!IMPORTANT]
> **One-Time Environment Setup for Any New PowerShell Terminal on This PC:**  
> Your PC has both Python 3.13 (Miniconda) and Python 3.11, as well as Java 23 and Java 17 (`C:\Tools\jdk-17`). Because PySpark 3.5, Delta Lake 3.2, and PyFlink 1.19 require **Python 3.11** and **Java 17**, paste these 4 lines at the start of your PowerShell terminal session (all tools and libraries are already installed on your machine!):
> ```powershell
> $env:JAVA_HOME="C:\Tools\jdk-17"
> $env:HADOOP_HOME="C:\hadoop"
> $env:PYSPARK_PYTHON="C:\Users\Admin\AppData\Local\Programs\Python\Python311\python.exe"
> $env:PATH="C:\Tools\jdk-17\bin;C:\Tools\apache-maven\bin;C:\Tools\sbt\bin;C:\hadoop\bin;C:\Users\Admin\AppData\Local\Programs\Python\Python311;C:\Users\Admin\AppData\Local\Programs\Python\Python311\Scripts;$env:PATH"
> ```

---

## P1: Spark Optimization — Data Skew & Salting Fix (`skew.py` & `fix.py`)

**Aim:** Implement a Spark job and use the Spark UI to identify and fix data skew and "shuffle" bottlenecks.

### Step-by-Step Setup & Verified Commands

**Step 1: Set environment and navigate to folder**
```powershell
$env:JAVA_HOME="C:\Tools\jdk-17"; $env:HADOOP_HOME="C:\hadoop"; $env:PYSPARK_PYTHON="C:\Users\Admin\AppData\Local\Programs\Python\Python311\python.exe"
$env:PATH="C:\Tools\jdk-17\bin;C:\hadoop\bin;C:\Users\Admin\AppData\Local\Programs\Python\Python311;C:\Users\Admin\AppData\Local\Programs\Python\Python311\Scripts;$env:PATH"
mkdir P1_Spark_Skew -Force
cd P1_Spark_Skew
```

**Step 2: Install PySpark (already installed in Python 3.11)**
```powershell
python -m pip install pyspark==3.5.3
python -c "import pyspark; print(pyspark.__version__)"
```

**Step 3: Create `skew.py` (Demonstrates 90% data skew on `key = 0`)**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import time

spark = SparkSession.builder.master("local[4]").appName("SparkSkew").config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.range(5000000).withColumn(
    "key", when(col("id") < 4500000, 0).otherwise((col("id") % 999) + 1)
)

df.groupBy("key").count().orderBy(col("count").desc()).show(10)

df.groupBy("key").count().collect()

print("Open Spark UI: http://localhost:4040")
time.sleep(120)
spark.stop()
```

**Step 4: Create `fix.py` (Fixes skew by salting `key = 0` across 8 buckets)**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import time

spark = SparkSession.builder.master("local[4]").appName("SparkFix").config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.range(5000000).withColumn("key", when(col("id") < 4500000, 0).otherwise(col("id") % 999 + 1))
df = df.withColumn("salt", when(col("key") == 0, col("id") % 8).otherwise(0))

result = df.groupBy("key", "salt").count().groupBy("key").sum("count")
result = result.withColumnRenamed("sum(count)", "count")
result.show(10)

print("Spark UI: http://localhost:4040")
time.sleep(120)
spark.stop()
```

**Step 5: Run `skew.py` and inspect Spark UI**
```powershell
python skew.py
```
*(While it pauses for 120 seconds, open `http://localhost:4040` $\rightarrow$ **Jobs / Stages** tab to observe 1 skewed task processing 4,500,000 rows.)*

**Step 6: Run `fix.py` and compare in Spark UI**
```powershell
python fix.py
```
*(Open `http://localhost:4040` $\rightarrow$ **Stages** tab to see the salted shuffle distribute `key = 0` across all 8 partitions.)*

**Verified Output:**
```text
+---+-------+
|key|  count|
+---+-------+
|  0|4500000|
|517|    501|
|518|    501|
...
+---+-------+
Open Spark UI: http://localhost:4040
```

---

## P2: Data Lakehouse — Delta Lake Time Travel & ACID (`delta_demo.py`)

**Aim:** Create a Delta Lake table and demonstrate "Time Travel" and "ACID Transactions" on Spark.

### Step-by-Step Setup & Verified Commands

**Step 1: Set environment and install `pyspark` & `delta-spark`**
```powershell
$env:JAVA_HOME="C:\Tools\jdk-17"; $env:HADOOP_HOME="C:\hadoop"; $env:PYSPARK_PYTHON="C:\Users\Admin\AppData\Local\Programs\Python\Python311\python.exe"
$env:PATH="C:\Tools\jdk-17\bin;C:\hadoop\bin;C:\Users\Admin\AppData\Local\Programs\Python\Python311;C:\Users\Admin\AppData\Local\Programs\Python\Python311\Scripts;$env:PATH"
mkdir P2_Delta_Lake -Force
cd P2_Delta_Lake
python -m pip install pyspark==3.5.3 delta-spark==3.2.1
```

**Step 2: Configure Windows Hadoop binaries (`winutils.exe` & `hadoop.dll` — already downloaded to `C:\hadoop\bin`)**
```powershell
mkdir C:\hadoop\bin -Force
curl.exe -L -o C:\hadoop\bin\winutils.exe https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe
curl.exe -L -o C:\hadoop\bin\hadoop.dll https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll
```

**Step 3: Create `delta_demo.py`**
```python
import os, shutil
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable

b = (SparkSession.builder.master("local[2]")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(b).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

p = "students_delta"
shutil.rmtree(p, ignore_errors=True)

spark.createDataFrame([(1,"Siddhant",90),(2,"Shreya",88),(3,"Shruti",75)],
                      ["id","name","marks"]).write.format("delta").mode("overwrite").save(p)
dt = DeltaTable.forPath(spark, p)
dt.update("id=3", {"marks":"80"})                           # version 1
dt.delete("id=2")                                           # version 2
dt.history().select("version","operation").show()
spark.read.format("delta").option("versionAsOf",0).load(p).show()   # TIME TRAVEL
try:                                                        # ACID / schema enforcement
    spark.createDataFrame([(9,"X",1,"extra")],["id","name","marks","c"]).write.format("delta").mode("append").save(p)
except Exception as e:
    print("Rejected:", type(e).__name__)
```

**Step 4: Run the Delta Lake script**
```powershell
python delta_demo.py
```

**Verified Output:**
```text
+-------+---------+
|version|operation|
+-------+---------+
|      2|   DELETE|
|      1|   UPDATE|
|      0|    WRITE|
+-------+---------+
+---+--------+-----+
| id|    name|marks|
+---+--------+-----+
|  1|Siddhant|   90|
|  2|  Shreya|   88|
|  3|  Shruti|   75|
+---+--------+-----+
Rejected: AnalysisException
```

---

## P3: 3-Broker Kafka Cluster in Docker + Producer/Consumer

**Aim:** Set up a 3-broker Kafka cluster using Docker and implement a Producer/Consumer in Python.

### Step-by-Step Setup & Verified Commands

**Step 1: Ensure Docker Desktop is running and create project folder**
```powershell
$env:PATH="C:\Users\Admin\AppData\Local\Programs\Python\Python311;C:\Users\Admin\AppData\Local\Programs\Python\Python311\Scripts;$env:PATH"
mkdir P3_Kafka_Cluster -Force
cd P3_Kafka_Cluster
docker --version
```

**Step 2: Install Python Kafka client library (`kafka-python`)**
```powershell
python -m pip install kafka-python
```

**Step 3: Create `docker-compose.yml`**
```yaml

services:
  kafka1:
    image: bitnamilegacy/kafka:3.7.1-debian-12-r4
    container_name: kafka1
    ports:
      - "29092:29092"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_KRAFT_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka1:9092,EXTERNAL://localhost:29092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    volumes:
      - kafka1_data:/bitnami/kafka

  kafka2:
    image: bitnamilegacy/kafka:3.7.1-debian-12-r4
    container_name: kafka2
    ports:
      - "39092:29092"
    environment:
      - KAFKA_CFG_NODE_ID=2
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_KRAFT_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka2:9092,EXTERNAL://localhost:39092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    volumes:
      - kafka2_data:/bitnami/kafka

  kafka3:
    image: bitnamilegacy/kafka:3.7.1-debian-12-r4
    container_name: kafka3
    ports:
      - "49092:29092"
    environment:
      - KAFKA_CFG_NODE_ID=3
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_KRAFT_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka1:9093,2@kafka2:9093,3@kafka3:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka3:9092,EXTERNAL://localhost:49092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    volumes:
      - kafka3_data:/bitnami/kafka

volumes:
  kafka1_data:
  kafka2_data:
  kafka3_data:
```

**Step 4: Create `producer.py`**
```python
from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092", "localhost:39092", "localhost:49092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(10):
    msg = {"student_id": i+1, "name": f"Student-{i+1}",
           "course": "Data Science"}
    producer.send("student-events", msg)
    print("Produced:", msg)
    time.sleep(1)

producer.flush()
producer.close()
```

**Step 5: Create `consumer.py`**
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "student-events",
    bootstrap_servers=["localhost:29092", "localhost:39092", "localhost:49092"],
    auto_offset_reset="earliest",
    group_id="student-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer started...")

for msg in consumer:
    print("Received:", msg.value)
```

**Step 6: Start the 3-broker Kafka cluster in Docker**
```powershell
docker rm -f kafka1 kafka2 kafka3
docker compose up -d
docker ps
```

**Step 7: Run Consumer (Terminal 1) and Producer (Terminal 2)**
```powershell
# In Terminal 1:
python consumer.py

# In Terminal 2:
python producer.py
```

**Step 8: Stop containers when done**
```powershell
docker compose down -v
```

---

## P4: Kafka Schema Registry — Backward Compatibility

**Aim:** Use Kafka Schema Registry to demonstrate a "Backward Compatible" schema change.

### Step-by-Step Setup & Verified Commands

**Step 1: Create project folder and install Confluent Avro & Schema Registry Python extras**
*(Note: Installing `"confluent-kafka[avro,schemaregistry]"` includes `authlib` and `httpx` required by `SchemaRegistryClient`)*
```powershell
$env:PATH="C:\Users\Admin\AppData\Local\Programs\Python\Python311;C:\Users\Admin\AppData\Local\Programs\Python\Python311\Scripts;$env:PATH"
mkdir P4_Schema_Registry -Force
cd P4_Schema_Registry
python -m pip install "confluent-kafka[avro,schemaregistry]" fastavro requests
```

**Step 2: Create `docker-compose.yml`**
```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.7.1
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
  schema-registry:
    image: confluentinc/cp-schema-registry:7.7.1
    depends_on: [kafka]
    ports: ["8081:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
```

**Step 3: Create `producer.py` (Handles both Schema V1 and Schema V2)**
```python
import sys
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

v1 = '{"type":"record","name":"Patient","namespace":"healthcare","fields":[{"name":"patient_id","type":"int"},{"name":"patient_name","type":"string"},{"name":"age","type":"int"}]}'
v2 = v1.replace('"type":"int"}]}', '"type":"int"},{"name":"blood_group","type":["null","string"],"default":null}]}')

v = sys.argv[1]
sr = SchemaRegistryClient({"url": "http://localhost:8081"})
p = SerializingProducer({"bootstrap.servers": "localhost:9092",
                         "key.serializer": StringSerializer("utf_8"),
                         "value.serializer": AvroSerializer(sr, v1 if v == "1" else v2)})
recs = ([{"patient_id":101,"patient_name":"Rahul","age":35},{"patient_id":102,"patient_name":"Priya","age":28}] if v == "1"
        else [{"patient_id":103,"patient_name":"Amit","age":42,"blood_group":"A+"}])
for r in recs:
    p.produce("healthcare-patients", key=str(r["patient_id"]), value=r); print("Produced V"+v, r)
p.flush()
```

**Step 4: Create `consumer.py`**
```python
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

sr = SchemaRegistryClient({"url": "http://localhost:8081"})
c = DeserializingConsumer({"bootstrap.servers":"localhost:9092","group.id":"hc","auto.offset.reset":"earliest",
                           "key.deserializer":StringDeserializer("utf_8"),"value.deserializer":AvroDeserializer(sr)})
c.subscribe(["healthcare-patients"])
while True:
    m = c.poll(1.0)
    if m and not m.error(): print("Received:", m.value())
```

**Step 5: Start Kafka and Schema Registry containers**
```powershell
docker compose up -d
# Wait ~10 seconds, then verify Schema Registry returns []:
curl.exe http://localhost:8081/subjects
```

**Step 6: Produce V1 data, set `BACKWARD` compatibility, and produce V2 data**
```powershell
python producer.py 1
Invoke-RestMethod -Method Put -Uri "http://localhost:8081/config/healthcare-patients-value" -ContentType "application/vnd.schemaregistry.v1+json" -Body '{"compatibility":"BACKWARD"}'
python producer.py 2
curl.exe http://localhost:8081/subjects/healthcare-patients-value/versions
```

**Step 7: Run Consumer to read both V1 and V2 messages**
```powershell
python consumer.py
```

**Verified Output:**
```text
Produced V1 {'patient_id': 101, 'patient_name': 'Rahul', 'age': 35}
Produced V1 {'patient_id': 102, 'patient_name': 'Priya', 'age': 28}
compatibility: BACKWARD
Produced V2 {'patient_id': 103, 'patient_name': 'Amit', 'age': 42, 'blood_group': 'A+'}
[1,2]
Received: {'patient_id': 101, 'patient_name': 'Rahul', 'age': 35}
Received: {'patient_id': 102, 'patient_name': 'Priya', 'age': 28}
Received: {'patient_id': 103, 'patient_name': 'Amit', 'age': 42, 'blood_group': 'A+'}
```

**Step 8: Stop containers when done**
```powershell
docker compose down -v
```

---

## P5: Spark Structured Streaming — Live Socket Word Count

**Aim:** Build a Spark Structured Streaming app to count word frequencies from a live socket stream.

### Step-by-Step Setup & Verified Commands

**Step 1: Set environment and navigate to folder**
```powershell
$env:JAVA_HOME="C:\Tools\jdk-17"; $env:HADOOP_HOME="C:\hadoop"; $env:PYSPARK_PYTHON="C:\Users\Admin\AppData\Local\Programs\Python\Python311\python.exe"
$env:PATH="C:\Tools\jdk-17\bin;C:\hadoop\bin;C:\Users\Admin\AppData\Local\Programs\Python\Python311;C:\Users\Admin\AppData\Local\Programs\Python\Python311\Scripts;$env:PATH"
mkdir P5_Word_Count_Stream -Force
cd P5_Word_Count_Stream
python -m pip install pyspark==3.5.3
```

**Step 2: Create `socket_sender.py`**
```python
import socket
s = socket.socket(); s.bind(("localhost", 9999)); s.listen(1)
print("Waiting for Spark on port 9999...")
c, _ = s.accept()
while (m := input("Enter: ")) != "exit":
    c.sendall((m + "\n").encode())
c.close()
```

**Step 3: Create `word_count_stream.py`**
*(Includes `.config("spark.sql.shuffle.partitions","2")` so each micro-batch runs 2 tasks in `<1 second` instead of 200 tasks)*
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
spark = SparkSession.builder.master("local[2]").appName("WC").config("spark.sql.shuffle.partitions","2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
d = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()
c = d.select(explode(split("value", " ")).alias("word")).groupBy("word").count()
c.writeStream.outputMode("complete").format("console").start().awaitTermination()
```

**Step 4: Run Socket Sender in Terminal 1**
```powershell
python socket_sender.py
```

**Step 5: Run Spark Structured Streaming in Terminal 2**
```powershell
python word_count_stream.py
```

**Step 6: Type text in Terminal 1 (`hello spark hello python hello spark`)**

**Verified Output (Terminal 2):**
```text
-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----+
|  word|count|
+------+-----+
| hello|    3|
|python|    1|
| spark|    2|
+------+-----+
```

---

## P6: Flink State Lab — Fraudulent Transactions (`KeyedProcessFunction`)
download java and maven if not installed in computer 
**Aim:** Implement a Flink application that detects "Fraudulent Transactions" (two large transactions within 1 minute) using `KeyedProcessFunction`.

### Step-by-Step Setup & Verified Commands

**Step 1: Set Java 17 & Maven environment and create project folder**
```powershell
$env:JAVA_HOME="C:\Tools\jdk-17"
$env:PATH="C:\Tools\jdk-17\bin;C:\Tools\apache-maven\bin;$env:PATH"
mkdir P6_Flink_Fraud -Force
cd P6_Flink_Fraud
java -version
mvn -version
```

**Step 2: Create `pom.xml` in `P6_Flink_Fraud\`**
```xml
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId><artifactId>flink-practical</artifactId><version>1.0</version>
  <properties><maven.compiler.release>17</maven.compiler.release><flink.version>1.19.2</flink.version></properties>
  <dependencies>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId><version>${flink.version}</version></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-clients</artifactId><version>${flink.version}</version></dependency>
  </dependencies>
  <build><sourceDirectory>${project.basedir}</sourceDirectory></build>
</project>
```

**Step 3: Create `FraudDetectionJob.java` in the same folder (`P6_Flink_Fraud\`)**
```java
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FraudDetectionJob {
  public static class Tx {
    public String id; public double amt; public long ts;
    public Tx() {}
    public Tx(String id, double amt, long ts) { this.id = id; this.amt = amt; this.ts = ts; }
  }
  public static class Detector extends KeyedProcessFunction<String, Tx, String> {
    ValueState<Long> last;
    public void open(Configuration c) {
      last = getRuntimeContext().getState(new ValueStateDescriptor<>("last", Types.LONG));
    }
    public void processElement(Tx t, Context ctx, Collector<String> out) throws Exception {
      if (t.amt < 10000) return;                       // only large transactions
      Long p = last.value();
      if (p != null && t.ts - p <= 60000)              // two large within 1 minute
        out.collect("FRAUD ALERT: " + t.id + ", gap=" + (t.ts - p) / 1000 + " s");
      last.update(t.ts);
    }
  }
  public static void main(String[] a) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromData(new Tx("C101",15000,10000), new Tx("C202",5000,20000), new Tx("C101",20000,45000),
                 new Tx("C202",12000,50000), new Tx("C101",3000,55000), new Tx("C101",18000,90000),
                 new Tx("C202",14000,130000))
       .keyBy(t -> t.id).process(new Detector()).print();
    env.execute("Fraud");
  }
}
```

**Step 4: Compile and execute in a forked JVM via Maven**
```powershell
mvn -q compile exec:exec "-Dexec.executable=java" "-Dexec.args=-cp %classpath FraudDetectionJob"
```

**Verified Output:**
```text
14> FRAUD ALERT: C101, gap=35 s
14> FRAUD ALERT: C101, gap=45 s
```

---

## P7: Watermark Handling & Late Data (PyFlink — 1 File)

**Aim:** Create a stream processor that correctly handles "Late Data" using watermarks and side outputs.

### Step-by-Step Setup & Verified Commands

**Step 1: Set Java 17 & Python 3.11 environment and install `apache-flink`**
```powershell
$env:JAVA_HOME="C:\Tools\jdk-17"
$env:PATH="C:\Tools\jdk-17\bin;C:\Users\Admin\AppData\Local\Programs\Python\Python311;C:\Users\Admin\AppData\Local\Programs\Python\Python311\Scripts;$env:PATH"
mkdir P7_Late_Data -Force
cd P7_Late_Data
python -m pip install apache-flink==1.19.1
```

**Step 2: Create `latedata.py`**
```python
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction
from pyflink.datastream.output_tag import OutputTag

W, D = 10_000, 2_000                      # 10s window, 2s max out-of-orderness
late_tag = OutputTag("late", Types.STRING())

class P(ProcessFunction):
    def __init__(self): self.mx = None
    def process_element(self, v, ctx):
        wm = None if self.mx is None else self.mx - D          # Current watermark
        end = (v[1] // W) * W + W                              # Window end timestamp
        if wm is not None and end - 1 <= wm:
            yield late_tag, "LATE: " + str(v)
        else:
            yield "MAIN: " + str(v)
        self.mx = v[1] if self.mx is None else max(self.mx, v[1])

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
ev = [("Picked up",1000), ("At restaurant",8000), ("Delivered",22000), ("Order confirmed",3000)]
m = env.from_collection(ev, type_info=Types.TUPLE([Types.STRING(), Types.LONG()])) \
       .process(P(), output_type=Types.STRING())
m.print()
m.get_side_output(late_tag).print()
env.execute("late")
```

**Step 3: Run the script**
```powershell
python latedata.py
```

**Verified Output:**
```text
MAIN: ('Picked up', 1000)
MAIN: ('At restaurant', 8000)
MAIN: ('Delivered', 22000)
LATE: ('Order confirmed', 3000)
```

---

## P8: Spark GraphX PageRank (Scala + SBT)

**Aim:** Use Spark GraphX to analyze a social network dataset and find the "Most Influential" nodes using PageRank.

### Step-by-Step Setup & Verified Commands

**Step 1: Set Java 17 & SBT environment and create directories**
```powershell
$env:JAVA_HOME="C:\Tools\jdk-17"
$env:HADOOP_HOME="C:\hadoop"
$env:PATH="C:\Tools\jdk-17\bin;C:\Tools\sbt\bin;C:\hadoop\bin;$env:PATH"
mkdir P8_GraphX\src\main\scala -Force
mkdir P8_GraphX\data -Force
cd P8_GraphX
```

**Step 2: Create `build.sbt` in `P8_GraphX\build.sbt`**
```scala
name := "GraphXPageRank"
scalaVersion := "2.12.18"
libraryDependencies ++= Seq("spark-core","spark-sql","spark-graphx").map("org.apache.spark" %% _ % "3.5.7")
fork := true
javaOptions ++= Seq("java.lang","java.lang.invoke","java.lang.reflect","java.io","java.net","java.nio",
  "java.util","java.util.concurrent","java.util.concurrent.atomic","sun.nio.ch","sun.nio.cs","sun.security.action","sun.util.calendar")
  .map(p => s"--add-opens=java.base/$p=ALL-UNNAMED")
```

**Step 3: Create `data\vertices.csv`**
```csv
id,name
1,Alice
2,Bob
3,Charlie
4,David
5,Emma
6,Frank
```

**Step 4: Create `data\edges.csv`**
```csv
src,dst
1,2
1,3
2,3
3,1
4,2
5,4
6,2
```

**Step 5: Create `src\main\scala\GraphXPageRank.scala`**
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object GraphXPageRank {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("PR").master("local[*]").getOrCreate().sparkContext
    sc.setLogLevel("ERROR")
    val users = sc.textFile("data/vertices.csv").filter(!_.startsWith("id,"))
      .map { l => val f = l.split(","); (f(0).toLong, f(1)) }
    val edges = sc.textFile("data/edges.csv").filter(!_.startsWith("src,"))
      .map { l => val f = l.split(","); Edge(f(0).toLong, f(1).toLong, 1) }
    val g = Graph(users, edges)
    println(s"Users: ${g.numVertices}, Relationships: ${g.numEdges}")
    val result = users.join(g.pageRank(0.0001).vertices).map { case (_, (n, r)) => (n, r) }.sortBy(_._2, false)
    result.collect().foreach(println)
    println("Most influential: " + result.first())
    sc.stop()
  }
}
```

**Step 6: Run with SBT**
```powershell
sbt run
```

**Verified Output:**
```text
[info] Users: 6, Relationships: 7
[info] (Charlie,2.1232774588227703)
[info] (Alice,1.9547208520626864)
[info] (Bob,1.3443056411388237)
[info] (David,0.27759420487145)
[info] (Frank,0.15005092155213512)
[info] (Emma,0.15005092155213512)
[info] Most influential: (Charlie,2.1232774588227703)
[success]
```

---

## P9: Distributed SQL with Trino (MySQL + TPC-H Federated Join)

**Aim:** Set up a Trino (Presto) coordinator and query data across two different sources simultaneously.

### Step-by-Step Setup & Verified Commands

**Step 1: Create project folders (`mysql` and `etc\catalog`)**
```powershell
mkdir P9_Trino\mysql -Force
mkdir P9_Trino\etc\catalog -Force
cd P9_Trino
```

**Step 2: Create `docker-compose.yml` (Uses host port `8085:8080` to avoid Windows port `8080` conflicts)**
```yaml
services:
  mysql:
    image: mysql:8.0
    container_name: trino_mysql
    environment: {MYSQL_ROOT_PASSWORD: root123, MYSQL_DATABASE: university}
    ports: ["3307:3306"]
    volumes: ["./mysql/init.sql:/docker-entrypoint-initdb.d/init.sql:ro"]
  trino:
    image: trinodb/trino:latest
    container_name: trino_coordinator
    ports: ["8085:8080"]
    volumes: ["./etc/catalog:/etc/trino/catalog"]
    depends_on: [mysql]
```

**Step 3: Create `mysql\init.sql`**
```sql
CREATE TABLE university.students (student_id INT PRIMARY KEY, student_name VARCHAR(100), department VARCHAR(100));
INSERT INTO university.students VALUES (101,'Aarav','Data Science'),(102,'Diya','Computer Science'),(103,'Rohan','Data Science'),(104,'Meera','Computer Science');
```

**Step 4: Create `etc\catalog\mysql.properties`**
```ini
connector.name=mysql
connection-url=jdbc:mysql://mysql:3306
connection-user=root
connection-password=root123
```

**Step 5: Create `etc\catalog\tpch.properties` (Second data source)**
```ini
connector.name=tpch
```

**Step 6: Start containers and execute the cross-source federated JOIN**
```powershell
docker rm -f trino_mysql trino_coordinator
docker compose up -d
# Wait ~15 seconds, then run:
docker exec -it trino_coordinator trino --execute "SELECT s.student_name, s.department, n.name AS nation FROM mysql.university.students s JOIN tpch.tiny.nation n ON s.student_id - 100 = n.nationkey;"
```

**Verified Output:**
```text
"Aarav","Data Science","ARGENTINA"
"Diya","Computer Science","BRAZIL"
"Rohan","Data Science","CANADA"
"Meera","Computer Science","EGYPT"
```

**Step 7: Stop containers when done**
```powershell
docker compose down -v
```

---

## P10: Real-Time Dashboard — Kafka $\rightarrow$ ClickHouse $\rightarrow$ Grafana (3 Files Only)

**Aim:** Integrate a Kafka stream with an OLAP engine (ClickHouse) and visualize the data on a Grafana Dashboard.

### Step-by-Step Setup & Verified Commands

**Step 1: Create project folder**
```powershell
mkdir P10_ClickHouse_Grafana -Force
cd P10_ClickHouse_Grafana
```

**Step 2: Create `docker-compose.yml`**
```yaml
services:
  kafka:
    image: apache/kafka:3.9.1
    ports: ["9092:9092"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports: ["8123:8123","9000:9000"]
    environment:
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
      CLICKHOUSE_PASSWORD: prac123
  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    environment:
      GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
  producer:
    image: python:3.11-slim
    working_dir: /app
    volumes: [".:/app"]
    command: sh -c "pip install -q kafka-python && python -u producer.py"
    restart: on-failure
    depends_on: [kafka]
  consumer:
    image: python:3.11-slim
    working_dir: /app
    volumes: [".:/app"]
    command: sh -c "pip install -q kafka-python clickhouse-connect && python -u consumer.py"
    restart: on-failure
    depends_on: [kafka, clickhouse]
```

**Step 3: Create `producer.py`**
```python
import json, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer
p = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode())
while True:
    r = {"event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
         "city": random.choice(["Mumbai","Pune","Nashik","Nagpur"]),
         "pm25": round(random.uniform(10,180),2), "pm10": round(random.uniform(20,250),2),
         "temperature": round(random.uniform(24,38),2)}
    p.send("air_quality", r); print("Sent:", r, flush=True); time.sleep(2)
```

**Step 4: Create `consumer.py`**
```python
import json, clickhouse_connect
from datetime import datetime
from kafka import KafkaConsumer
ch = clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password="prac123")
ch.command("CREATE TABLE IF NOT EXISTS default.air_quality(event_time DateTime, city String, pm25 Float64, pm10 Float64, temperature Float64) ENGINE=MergeTree ORDER BY (city,event_time)")
cols = ["event_time","city","pm25","pm10","temperature"]
for m in KafkaConsumer("air_quality", bootstrap_servers="kafka:9092", auto_offset_reset="earliest",
                       value_deserializer=lambda v: json.loads(v)):
    m.value["event_time"] = datetime.strptime(m.value["event_time"], "%Y-%m-%d %H:%M:%S")
    ch.insert("default.air_quality", [[m.value[k] for k in cols]], column_names=cols)
    print("Inserted:", m.value, flush=True)
```

**Step 5: Start all 5 containers and verify data in ClickHouse**
```powershell
docker compose down -v
docker compose up -d
# Wait ~20 seconds, then check logs and ClickHouse rows:
docker compose logs --tail=10 producer
docker compose logs --tail=10 consumer
docker compose exec clickhouse clickhouse-client --password prac123 --query "SELECT * FROM default.air_quality ORDER BY event_time DESC LIMIT 5"
```

**Verified Output:**
```text
2026-11-18 16:50:51	Nashik	153.77	226.3	36.3
2026-11-18 16:50:49	Mumbai	128.7	228.31	29.14
2026-11-18 16:50:47	Nashik	87.27	99.81	33.96
2026-11-18 16:50:45	Nagpur	123.99	242.31	24.18
```

**Step 6: Visualize in Grafana (`http://localhost:3000`)**
1. Login at `http://localhost:3000` with `admin` / `admin`.
2. **Connections $\rightarrow$ Data sources $\rightarrow$ Add data source $\rightarrow$ ClickHouse**:
   - **Server address:** `clickhouse` | **Server port:** `9000` (Native) or `8123` (HTTP)
   - **Username:** `default` | **Password:** `prac123` | **Default database:** `default` $\rightarrow$ **Save & test**.
3. **Dashboards $\rightarrow$ New $\rightarrow$ Add visualization $\rightarrow$ SQL Editor**:
```sql
SELECT
  $__timeInterval(event_time) AS time,
  city,
  avg(pm25) AS pm25
FROM default.air_quality
WHERE $__timeFilter(event_time)
GROUP BY time, city
ORDER BY time
```

**Step 7: Stop containers when done**
```powershell
docker compose down -v
```
