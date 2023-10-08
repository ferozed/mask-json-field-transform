# 1.1

Added support for `map`. Usually, the SMT will get map as an input when the connector is using a `JsonConverter` with `schemas.enable`=`false`.

# 1.0

Release version. No changes from 0.3

# 0.3

Fixed bugs. We no longer replace with empty string if the value is not a string. The replacement value
is chosen depending on the type of the field in the json.

# 0.2

Initial version