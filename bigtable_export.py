#!/usr/bin/env python3

"""
Dump all tables in a given Bigtable instance to a GCS bucket, as a series of Hadoop sequence files.
"""

__author__ = "Maciej Sieczka <msieczka@egnyte.com>"

import argparse
import time
import logging
import subprocess
import re
from google.cloud import bigtable, storage

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=logging.WARNING)


def main():
    date = str(time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime()))

    # In case an env GOOGLE_APPLICATION_CREDENTIALS variable is not the preferred way, one could hardcode it as follows,
    # but Java will still need it to run the java-bigtable-hbase shaded jar!
    # bigtable_client = bigtable.Client.from_service_account_json(args.auth_json_path, project=args.gcp_project, admin=True)
    bigtable_client = bigtable.Client(project=args.gcp_project, admin=True)
    storage_client = storage.Client(project=args.gcp_project)

    instance = bigtable_client.instance(args.bigtable_instance_id)
    instance.reload()

    cluster = instance.cluster(args.bigtable_cluster_id)
    cluster.reload()

    bucket = storage_client.get_bucket(args.bucket_name)

    # A "Development" BT instance will return `0` nodes. We need to use at least 1 node, but on "Production" instances
    # let's leave one alone, to reduce the overall load on the instance.
    node_count = str(1 if not cluster.serve_nodes else cluster.serve_nodes - 1)
    cluster_zone = cluster.location_id
    cluster_region = '-'.join(cluster_zone.split('-')[0:-1])

    tables_bt = (t.name.split('/')[-1] for t in instance.list_tables())

    # We have tables from multiple envs on a single BT instance. Backup should normally include only the tables specific
    # for a given env.
    tables = (t for t in tables_bt if t.startswith(args.table_id_prefix))
    logging.warning('Bigtable backup started.')

    for table_short_name in tables:
        logging.warning('Exporting table "{}" from Bigtable instance "{}" to GCS at "{}".'.
                        format(table_short_name, args.bigtable_instance_id, args.bucket_name+'/'+date+'/'+table_short_name+'/'))

        column_families = [*instance.table(table_short_name).list_column_families().keys()]
        blob = bucket.blob(date+'/'+table_short_name+'/'+table_short_name+'.families')
        blob.upload_from_string('\n'.join(column_families))

        # TODO: This spawns individual Dataflow job for each table, which is quite an overhead.
        #  Try replacing the jar with our own code using Apache Beam (https://pypi.org/project/apache-beam/) Python SDK
        #  (https://github.com/apache/beam/tree/master/sdks/python)? Or hope for
        #  https://github.com/googleapis/cloud-bigtable-client/issues/2180 ("Add support for exporting from multiple
        #  tables at once") getting done by the upstream, so that the bigtable-beam-import.jar accepts *multiple*
        #  `bigtableTableId`'s and processes them in *sequence*, in a *single Dataflow job*.
        subprocess.check_call(['/usr/bin/java', '-jar', args.beam_jar_path, 'export',
                               '--runner=dataflow',
                               '--project='+args.gcp_project,
                               '--bigtableInstanceId='+args.bigtable_instance_id,
                               '--bigtableTableId='+table_short_name,
                               '--destinationPath='+'gs://'+args.bucket_name+'/'+date+'/'+table_short_name,
                               '--tempLocation='+'gs://'+args.bucket_name+'/jar-temp',
                               '--maxNumWorkers='+node_count,
                               '--diskSizeGb=30',
                               '--sdkWorkerParallelism=0',
                               '--workerMachineType=n1-highcpu-8',
                               '--jobName=bt-backup-'+date+'-'+re.sub('[^-a-z0-9]', '-', table_short_name.lower()),
                               '--region='+cluster_region,
                               '--bigtableMaxVersions=1'])

    blob = bucket.blob(date+'/'+'this_backup_went_ok')
    blob.upload_from_string('')
    logging.warning('Bigtable backup completed successfully.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.HelpFormatter)
    required = parser.add_argument_group('required arguments')
    required.add_argument('--beam_jar_path', help='Path to the Bigtable HBase client jar file.', required=True)
    required.add_argument('--gcp_project', help='ID of the Bigtable instance parent GCP project.', required=True)
    required.add_argument('--bigtable_instance_id', help='ID of the Bigtable instance.', required=True)
    required.add_argument('--bigtable_cluster_id', help='ID of the cluster in the Bigtable instance.', required=True)
    required.add_argument('--bucket_name', help='GCS bucket name to dump the Bigtable tables into. The Hadoop sequence '
                                                'files will be saved in a directory named after the export start time, '
                                                'in format `YYYY-mm-dd-HH-MM-SS` (eg. `2019-11-02-10-00-16/`), with '
                                                'each table contents in a sudbirectory there, named after the table '
                                                'name.', required=True)
    parser.add_argument('--table_id_prefix', help='Backup only the tables with this prefix in their ID.',
                        required=False, default='')
    # required.add_argument('--auth_json_path', help='Path to the Google credentials JSON file with a service account key.', required=True)
    args = parser.parse_args()

    main()
