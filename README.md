Persistent append-only log with error detection:

Computes checksum of len(data) & data, and prepends that as a header for a log entry.
When reading the log file, if a stored checksum does not match the checksum we compute,
throw away that entry and any after.

