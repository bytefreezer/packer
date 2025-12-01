# Randomized Load Balancing for Housekeeping

## Overview

The housekeeping system now uses randomized intervals instead of fixed intervals to provide automatic load balancing across multiple instances.

## How it Works

### Base Interval
- Set via `housekeeping.intervalseconds` configuration (e.g., 60 seconds)
- This becomes the minimum interval between housekeeping cycles

### Randomization
- Each housekeeping cycle generates a random multiplier between 1.0 and 2.0
- Next interval = baseInterval × randomMultiplier
- Results in intervals between 1x and 2x the base interval

### Initial Spread
- Each instance gets an initial random delay (0 to baseInterval)
- Prevents synchronized startup when multiple instances start together

## Example

With `housekeeping.intervalseconds: 60`:

```
Instance 1:
- Initial delay: 23s
- Cycle 1: 84s wait (1.4x multiplier)
- Cycle 2: 112s wait (1.87x multiplier)  
- Cycle 3: 67s wait (1.12x multiplier)

Instance 2:
- Initial delay: 41s
- Cycle 1: 93s wait (1.55x multiplier)
- Cycle 2: 118s wait (1.97x multiplier)
- Cycle 3: 74s wait (1.23x multiplier)
```

## Benefits

1. **Automatic Load Distribution**: No manual configuration needed
2. **Collision Avoidance**: Multiple instances naturally spread out
3. **Resource Smoothing**: Prevents thundering herd effects
4. **Predictable Bounds**: Always between 1x-2x base interval
5. **Simple Configuration**: Uses existing interval setting

## Migration

No configuration changes needed! The system automatically provides load balancing while maintaining the same average processing frequency.

## Logging

The system logs randomization details:
```
INFO Starting housekeeping with base interval 1m0s (randomized 1x-2x for load balancing)
DEBUG Initial housekeeping delay: 23.456s
DEBUG Next housekeeping in 1m24s (base: 1m0s, multiplier: 1.40)
```

## Comparison with Fixed Intervals

### Before (Fixed)
- All instances run every 60s exactly
- Synchronized load spikes
- Manual staggering required

### After (Randomized)  
- Instances run every 60-120s randomly
- Natural load distribution
- Zero configuration needed