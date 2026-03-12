# Project TODOs

## General
- **TODO**: Rename PersonDataLoad to DataLakeDataLoad

## Resources (config/resources.py)
- **TODO**: Remove AsyncpgPoolResource as we no longer use COPY operations
- **TODO**: Add "HOUSING_STAGE"."ETL_CURR_HOUSING_IDS" resource

## Course Flow (flows/course_raw_flow.py)
- **TODO**: Call Get Course Offerings with term first, and merge with course details

## Person Flow (flows/person_raw_flow.py)
- **TODO**: Fetch ENS population too, or call EVERY Population
- **TODO**: Re-enable VDS BUID fetch after new credentials are set up (currently commented out)
- **TODO**: Any failed BUIDs will go into the person_live_update queue for reprocessing
- **TODO**: Add logic for Faculty Terms. Consider if buid is a student and faculty for Terms
- **TODO**: Snap takes about 5 minutes to finish. Timeout after 11 minutes. Log any buids that failed or insert them into queue to redo (live updates queue)
- **TODO**: Temp fix for glitch in SnapLogic returning empty objects (filter persons by personid)
- **TODO**: Run uidcarterms evenly across all semaphores if psqueries are small. Will be important once we implement live updates
- **TODO**: Review redundant logic in person flow (maybe replace with asyncio.TaskGroup())

## Deployment Configuration

### Kubernetes Resource Requirements

Current resource allocations:
- **term_raw_flow**: requests (cpu: 1000m, memory: 2Gi), limits (cpu: 2000m, memory: 3Gi)
- **course_raw_flow**: requests (cpu: 1000m, memory: 2Gi), limits (cpu: 2000m, memory: 3Gi)
- **person_raw_flow**: requests (cpu: 1000m, memory: 4Gi), limits (cpu: 2000m, memory: 6Gi)

These should be configured at the infrastructure level (Kubernetes job template, work pool configuration, or deployment infrastructure settings).

### Concurrency Configuration

Concurrency is controlled through:
- Work pool concurrency limits
- Flow run concurrency limits
- Task runner configuration
