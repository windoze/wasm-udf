# WASM Function Wrapper
class WasmUdf(object):
    def __init__(self, wasm_bytes, name):
        self.wasm_bytes = wasm_bytes
        self.name = name
        self.engine = None
        self.store = None
        self.module = None
        self.instance = None
        self.f = None
    
    # Customize pickle, save and restore `wasm_bytes` and `name` only, which are a simple strings, other fields cannot be pickled
    def __getstate__(self):
        return (self.wasm_bytes, self.name)
    def __setstate__(self, state):
        self.wasm_bytes, self.name = state
    
    def __call__(self, *args):
        # Unpickling will **not** call `__init__`, we need to test if the wrapper has been initialized on the 1st invocation
        if not hasattr(self, "instance") or self.instance is None:
            # We need to re-JIT WASM function after unpickling
            # Import WASM modules here to make sure it gets called in executors
            from wasmer_compiler_cranelift import Compiler
            from wasmer import engine, Store, Instance, Module
            self.engine = engine.JIT(Compiler)
            self.store = Store(self.engine)
            self.module = Module(self.store, self.wasm_bytes)
            self.instance = Instance(self.module)
            self.f = getattr(self.instance.exports, self.name)
        return self.f(*args)

# Create WASM function instance
wasm_bytes = open('rust-udf/target/wasm32-unknown-unknown/release/rust_udf.wasm', 'rb').read()
rust_add = WasmUdf(wasm_bytes, "rust_add")

# Test if pickle works
import pickle
ps = pickle.dumps(rust_add)
my_add1 = pickle.loads(ps)
assert(my_add1(7, 35)==42)

# Use WASM function in Spark
from pyspark.sql.functions import udf, lit, col
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('WASMTest').getOrCreate()

# Create a DataFrame
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]
columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

# Use WASM UDF with DataFrame
myAddUDF = udf(lambda x, y: rust_add(x, y), IntegerType())
df.select(col("firstname"), col("middlename"), col("lastname"), col("salary"), myAddUDF(col("salary"), lit(1000)).alias("raised_salary")).show(truncate=False)

# Use WASM UDF in SparkSQL
spark.udf.register("rustAdd", rust_add, IntegerType())
df.createOrReplaceTempView("table1")
df2 = spark.sql("SELECT firstname, middlename, lastname, salary, rustAdd(salary, 1000) as raised_salary from table1")
df2.show(truncate=False)
