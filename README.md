# Concurrency

Wraps the concurrent.futures module
  * ThreadPools return full stack traces
  * Optional progress bar

```python
from concurrency import Concurrency, Task

def some_fn(data):
    return data

inputs = (0, 1, 2, 3)

# Threaded Run
concurrent = Concurrency(some_fn)
results = concurrent.run(inputs)
print results
# [0, 1, 2, 3]

```

# Install

pip install git+git://github.com/heyglen/Concurrency.git#egg=Concurrency

# Uninstall

pip uninstall concurrency -y
