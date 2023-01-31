# New Versions

All new work should be done in a feature branch, against the next snapshot build.

# Releasing

- Remove the `-SNAPSHOT` qualifier from the version string.
- Publish to maven ( see instructions below )
- Merge to master.


# Publish To Maven

## Prerequisites

Publication can only be done by the Owner.

```bash
./gradlew clean build publish
```

