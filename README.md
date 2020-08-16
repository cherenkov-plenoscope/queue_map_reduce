# qsub map and reduce [![Build Status](https://travis-ci.org/cherenkov-plenoscope/sun_grid_engine_map.svg?branch=master)](https://travis-ci.org/cherenkov-plenoscope/sun_grid_engine_map)

In embarrassingly parallel cumputing, jobs are mapped to worker-nodes, and later the individual results are reduced. This python-package does the mapping and the reduction for you.

## Example
```python
import sun_grid_engine_map as qmr
import numpy

results = qmr.map(
    function=numpy.sum,
    jobs=[numpy.arange(i, 100+i) for i in range(10)])
```

## Requirements
- Both the ```jobs```, and the ```results``` must be serializable using pickle.
- The ```function``` must be part of an importable python module.
- ```qstat```
- The SUN-grid-engine (SGE) qsub flavour. Currently this only works for SGE.
