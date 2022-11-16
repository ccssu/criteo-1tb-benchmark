import os
from pathlib import Path
import findspark
import sys
FILE = Path(__file__).resolve()
ROOT = FILE.parents[0]  # YOLOv5 root directory
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))  # add ROOT to PATH
ROOT = Path(os.path.relpath(ROOT, Path.cwd()))  # relative

from codesutils import ensure_directory_exists
findspark.init('/data/dataset/fengwen/script/data/spark-3.3.1-bin-hadoop3-scala2.13')
from pyspark import SparkContext
sc = SparkContext('local','pyspark')



criteo_data_remote_path = 'criteo/plain'
libsvm_data_remote_path = 'criteo/libsvm'
vw_data_remote_path = 'criteo/vw'

local_data_path = 'criteo/data'
local_results_path = 'criteo/results'
local_runtime_path = 'criteo/runtime'



criteo_day_template = os.path.join(criteo_data_remote_path, 'day_{}')
libsvm_day_template = os.path.join(libsvm_data_remote_path, 'day_{}')
vw_day_template = os.path.join(vw_data_remote_path, 'day_{}')

libsvm_train_template = os.path.join(libsvm_data_remote_path, 'train', '{}')
libsvm_test_template = os.path.join(libsvm_data_remote_path, 'test', '{}')
vw_train_template = os.path.join(vw_data_remote_path, 'train', '{}')
vw_test_template = os.path.join(vw_data_remote_path, 'test', '{}')

local_libsvm_test_template = os.path.join(local_data_path, 'data.test.{}.libsvm')
local_libsvm_train_template = os.path.join(local_data_path, 'data.train.{}.libsvm')
local_vw_test_template = os.path.join(local_data_path, 'data.test.{}.vw')
local_vw_train_template = os.path.join(local_data_path, 'data.train.{}.vw')


days = list(range(0, 23 + 1))
train_samples = [
    10000, 30000,  # tens of thousands
    100000, 300000,  # hundreds of thousands
    1000000, 3000000,  # millions
    10000000, 30000000,  # tens of millions
    100000000, 300000000,  # hundreds of millions
    1000000000, 3000000000,  # billions
]
test_samples = [1000000]
total_cores = 256
executor_cores = 4
executor_instances = total_cores / executor_cores
memory_per_core = 4
app_name = 'Criteo experiment'

master = 'yarn'

settings = {
    'spark.network.timeout': '600',
    
    'spark.driver.cores': '16',
    'spark.driver.maxResultSize': '16G',
    'spark.driver.memory': '32G',
    
    'spark.executor.cores': str(executor_cores),
    'spark.executor.instances': str(executor_instances),
    'spark.executor.memory': str(memory_per_core * executor_cores) + 'G',
    
    'spark.speculation': 'true',
    'spark.yarn.queue': 'root.HungerGames',
}

from pyspark.sql import SparkSession


builder = SparkSession.builder

builder.appName(app_name)
builder.master(master)
for k, v in settings.items():
    builder.config(k, v)

spark = builder.getOrCreate()
sc = spark.sparkContext

sc.setLogLevel('ERROR')

import sys
import logging

from importlib import reload # 添加
logging.shutdown()            # 添加
reload(logging)              # 在 reload(logging) 前添加两行代码


handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(message)s')
handler.setFormatter(formatter)

ensure_directory_exists(local_runtime_path)
file_handler = logging.FileHandler(filename=os.path.join(local_runtime_path, 'mylog.log'), mode='a')
file_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)
logger.info('Spark version: %s.', spark.version)