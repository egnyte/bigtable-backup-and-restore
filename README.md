# Description
`bigtable_export.py` and `bigtable_import.py` are Python wrappers for the [java-bigtable-hbase](
https://github.com/googleapis/java-bigtable-hbase) library and client in the form of an executable shaded jar,
available at https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-beam-import/. Both scripts require the
user to specify the jar's location via `--beam_jar_path` parameter.

The jar spawns Dataflow jobs to export/import Bigtable contents to/from a GCS bucket directory, 1 job per each input
table (as per the jar's limitation - see the [respective feature request](
https://github.com/googleapis/cloud-bigtable-client/issues/2180)), 1 job at a time (to avoid overloading the Bigtable
cluster). The directory is named after the export start time, in `YYYY-mm-dd-HH-MM-SS format`. Each table is dumped as
a series of Hadoop sequence files into its sudbirectory, named after the table name.

When ran for the 1st time per a given bucket (in either the import or export mode), the jar uploads itself to the
bucket's `jar-temp/staging/` location. Dataflow takes it from there and deploys it on its nodes, according to the
parameters specified on scripts' command line. See the output of `bigtable_export.py --help` and `bigtable_import.py
--help` for a list of the supported parameters.

More information, including the story behind this, is available in my [Bigtable backup for disaster recovery](
https://medium.com/egnyte-engineering/bigtable-backup-for-disaster-recovery-9eeb5ea8e0fb) blog post, on [Egnyte's](
https://www.egnyte.com/) [engineering blog](https://medium.com/egnyte-engineering).

## How to use this
### Prerequisites:

* In Google Cloud Console, download the JSON file for your GCP Service Account with permissions for reading
(`bigtable_export.py`) and writing (`bigtable_import.py`) your Bigtable cluster, running Dataflow jobs (both scripts)
and for writing (`bigtable_export.py`) and reading (`bigtable_import.py`) your GCS bucket.
* Make sure the Dataflow API is enabled in the project where your input/output Bigtable cluster is located.
* I'm assuming your input/output GCS bucket is in the same project where your Dataflow jobs will run. If you
need to export/import Bigtable data cross-project, follow the [Accessing Cloud Storage buckets across Google Cloud
Platform projects](
https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#accessing_cloud_storage_buckets_across_google_cloud_platform_projects)
instructions.

### Running the scripts:

#### 1. Directly on your machine:
  * Have Java 8 in `/usr/bin/java`. The jar won't work with a newer Java.  
  * `git clone` this repository.
  * `cd` to a directory where you have cloned it.
  * Run `pip install --requirement --user ./requirements.txt`.
  * Run `export GOOGLE_APPLICATION_CREDENTIALS=<path to the Service Account JSON file on your machine>`.
  * Use the scripts as per `bigtable_export.py --help` and `bigtable_import.py --help`.

#### 2. In a container, using our Docker image:
  * `git clone` this repository.
  * `cd` to a directory where you have cloned it.
  * Build the Docker image: `docker build --tag bigtable-backup-and-restore .` (**don't miss the dot at the end**).
  * Run a Bash session in your container, mounting the GCP Service Account JSON file inside it: `docker run
  --volume=<path to the Service Account JSON file on your machine>:/root/service_account.json --rm --interactive --tty
  bigtable-backup-and-restore:latest /bin/bash`.
  * In the container's Bash session:
     * Run `export GOOGLE_APPLICATION_CREDENTIALS=/root/service_account.json`.
     * Run the scripts as per `bigtable_export.py --help` and `bigtable_import.py --help`,  with `--beam_jar_path` set
     to `/usr/local/lib/bigtable-beam-import.jar`.

## How to contribute
A couple things could be improved in the scripts, e.g.:
  * The hardcoded Dataflow machine types, optimal for backup and restore performance with our Bigtable data. Make this
  customizable?
  * The number of cell versions to be exported fixed at 1. We don't need more, yet. Maybe you do?
  * No support for specific table names. Prefix wildcards work best for us, so far. A nice option to have though.
  * Hardcoded Google Python libraries versions in `requirements.txt`. I haven't tried the scripts against the newer
  ones, and they might have breaking changes. But update is in order, sooner or later.

And of course there still might be bugs lurking, even though we've used this tooling at Egnyte for months now, without
issues.

So please don't hesitate - **bug reports, feature requests and pull requests are most welcome!**

## License
MIT
