# How to write good benchmarks

## Properties of good benchmarks

* accessible dataset
* list all software verions
* list all hardware specs
* open source benchmarking code
* don't use any methdologies that clearly favor one engine without disclosing

## Benchmarking is hard

It's difficult to build accurate benchmarks.  Runtimes depends on the hardware, software versions, and data setup.

Accurate benchmarks are even harder when comparing different technologies.  Certain frameworks will perform better with different files sizes and file formats.  This benchmarking analysis tries to give a fair representation on the range of outcomes that are possible given the most impactful inputs.

The benchmarks presented in this repo should not be interpreted as definitive results.  They're runtimes for specific data tasks, on one type of hardware, with a specific set of software versions.  The code isn't necessarily optimized (we accept community contributions to restructure code).

The data community should find these benchmarks valuable, caveats aside.
