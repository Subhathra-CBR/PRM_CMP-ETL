This ETL pipeline extracts incremental participant and visit data from the PRM PostgreSQL database and loads it into the CMP PostgreSQL database.

The pipeline supports:

Incremental data loading using watermark control,
Participant synchronization,
Visit synchronization,
Upsert logic using PostgreSQL ON CONFLICT,
Environment variable-based configuration and
Data migration between PRM and CMP systems

The ETL pipeline validates mandatory environment variables,
Rolls back CMP transactions upon failure,
Logs error messages to the console and
Closes database connections in the finally block.

For testing purposes, the ETL currently processes data only for
participant_id IN (1132,1133).
Will remove the filter in final deployment.
Yet to update the mappings discussed on 4th sep2026.Need to extract the Appointment with BVA logic.(Barcode Visit Assessment). 
