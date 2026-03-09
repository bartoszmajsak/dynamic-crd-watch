# Migration

## The controller-runtime problem

This PoC runs on controller-runtime **v0.23.1**. The dynamic watch pattern depends on two APIs that landed in **v0.20.0**:

- `cache.Options.ReaderFailOnMissingInformer` - tells the cache to fail loudly when reading from a removed informer, instead of silently spawning a new one that blocks on sync forever.
- `cache.ErrResourceNotCached` - the error type returned when that happens, so the watcher can detect the race and reset gracefully.

Without these, `RemoveInformer` (available since v0.18.0) is not safe to use in practice. A concurrent read after removal silently starts a ghost informer that deadlocks the controller worker goroutine. Not great.

KEDA v2.17.3 breaks on controller-runtime >= v0.20.0 ([kedacore/keda#6660](https://github.com/kedacore/keda/issues/6660)).

## Options

**Wait for KEDA** - once KEDA ships a release that compiles against v0.20.0+, the replace can be dropped. This is the cleanest path - no hacks, no forks, no shims.

**Build a cache wrapper for v0.19.1** - you could wrap the cache read path to detect removed informers yourself. Essentially reimplementing what upstream added in v0.20.0. It works, but you're carrying code that exists upstream and will need to be ripped out later.

**Fork controller-runtime** - cherry-pick just the `ReaderFailOnMissingInformer` commit into a v0.19.1 fork. Same maintenance burden as the wrapper, arguably worse because now you own a fork of a fast-moving dependency.
