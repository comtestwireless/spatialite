# spatialite

This repository is organized (for convenience) in branches.

## main

Includes this readme and the GitHub Actions configuration to synchronize with the original FOSSIL repository.

## master

This branch includes a perfect mirror of the trunk branch of the original FOSSIL repository.

This repository gets updated daily from upstream.

Moreover, until the mantainer of the original repository starts doing this himself, all versions from 5 on are tagged manually.

## conan-build

This branch include small changes to make the original code buildable using conan.io dependencies instead of the system-wide package manager.

At the time of writing, it also includes an additional function called get_location which is used to easily find out where the dll is at runtime. It will eventually be removed.
