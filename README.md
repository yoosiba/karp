# karp  >< ( ( ( *>
## kafka republish utils


This is small Java replacement for a bunch of Bash scripts used when preparing data for re-publishing events to Kafka.

Use case: Some system failed for a bunch of random kafka messages. You need to republish them. Assuming events are in a plain text, maybe JSON, you consume and produce with kafkacat. Procedure might look like this:
1. Obtain list of some unique IDs for failed messages. Lets say `event_id`.
2. Dump kafka messages to the disk. Potentially multiple files.
3. Filter all data files by all unique IDs.
4. Replace some part of the event, e.g generate new random `UUID` in place of previously filtered `event_id`.
5. Write all new generated events to the new file.
6. Save in a separate file all new generated IDS. Allows easier check later if published events were processed by the system.
7. Publish new generated events.

This code helps with steps 3-6.

Usage:
1. Create some working directory, e.g. `~/incident`
2. put data files with events in `~/incident/old` directory
3. put files with IDs in `~/incident/ids` directory
4. run Karp with `~/incident` as argument.

Karp will read all data files into memory, do all the searching & filtering, data replacement and will generate following:
1. `~/incident/new_events.txt` with old events but with IDs changed to new random `UUID`s.
2. `~/incident/new_ids.txt` with `UUID`s that were previously generated and used in `new_events.txt`.

Note that Karp reads all data into memory so depending on total size of your files consider running with `-Xmx4G` or `-Xmx10G`, etc.

### Two modes of operation.
Karp has two modes:
* `raw` which is fast and dumb
* `semantic` which does contextual ID matching, but is slower.

`raw` mode is basically replacing all occurrences of a given `UUID` with new `UUID`.

`semantic` will match `"event_id" : "UUID_VALUE"` and will replace `UUID_VALUE`. This will *not* match parts like `"triggering_system_event_id" : "UUID_VALUE"`, even if the `UUID` is the same. This might or might not be desired. Decide.

To activate semantic mode add `semantic` as **second** argument.

### Build system...?
Nah. I run it from Eclipse. No need for dependency management or packaging.
But you do need **Java 14**. If you are using earlier versions, you will need to adjust few lines. Nothing major, just few convenience APIs.
