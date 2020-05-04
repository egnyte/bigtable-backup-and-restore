#!/usr/bin/env python3

"""
Restore series of Hadoop sequence files in a GCS bucket as Bigtable tables.
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

    node_count = str(cluster.serve_nodes or 1)
    cluster_zone = cluster.location_id
    cluster_region = '-'.join(cluster_zone.split('-')[0:-1])

    # MaxVersionsGCRule(1) gives "versions() > 1" on `cbt -project <project> -instance <instance> ls <table-id>` output.
    max_versions = bigtable.column_family.MaxVersionsGCRule(1)

    successful_backup_marker = bucket.blob(args.backup_gcs_dir+'/'+'this_backup_went_ok')

    # Extract the parent "directories" of Bigtable dump bucket blobs ("files"). A "directory" is named the same as the
    # table that was dumped there. So we can use this name in table backup restoration.
    tables_gcs = frozenset(b.name.split('/')[1] for b in bucket.list_blobs(prefix=args.backup_gcs_dir,
                                                                           fields='kind,items(name),nextPageToken')) \
                 - frozenset([successful_backup_marker.name.split('/')[1]])

    # Fetch names of the tables already there in the Bigtable instance.
    tables_bt = frozenset(t.name.split('/')[-1] for t in instance.list_tables())

    # We'll skip import for GCS table dumps which have the same name as a table on the destination Bigtable instance.
    tables_common = tables_gcs.intersection(tables_bt)
    tables = tables_gcs - tables_common

    if tables_common:
        logging.warning('WATCH OUT!!!')
        logging.warning('The following input tables already exist in the destination Bigtable instance "{}": {}'.
                        format(args.bigtable_instance_id, ', '.join(tables_common)))
        logging.warning('They will NOT be imported. If you want to restore them from backup anyway, remove them from '
                        'the destination Bigtable instance 1st.')
        logging.warning('WATCH OUT!!!')

    if not successful_backup_marker.exists() and not args.force:
        logging.warning('There\'s no successful backup marker blob in GCS at {}. To restore from this location anyway, '
                        'run the script with `--force` flag.'.format(args.bucket_name+'/'+successful_backup_marker.name))
    else:
        for table_short_name in tables:
            source_pattern = args.bucket_name+'/'+args.backup_gcs_dir+'/'+table_short_name+'/part-*'

            logging.warning('Importing table "{}" to Bigtable instance "{}" from GCS at "{}".'.
                            format(table_short_name, args.bigtable_instance_id, source_pattern))

            blob = bucket.get_blob(args.backup_gcs_dir+'/'+table_short_name+'/'+table_short_name+'.families')
            column_families = {k: max_versions for k in blob.download_as_string().decode('utf-8').split('\n')}
            table = instance.table(table_short_name)
            table.create(column_families=column_families)

            # TODO: This spawns individual Dataflow job for each table, which is quite an overhead.
            #  Try replacing the jar with our own code using Apache Beam (https://pypi.org/project/apache-beam/) Python
            #  SDK (https://github.com/apache/beam/tree/master/sdks/python)? Or hope for
            #  https://github.com/googleapis/cloud-bigtable-client/issues/2180 ("Add support for exporting from multiple
            #  tables at once") getting done by the upstream, so that the bigtable-beam-import.jar accepts *multiple*
            #  `bigtableTableId`'s and processes them in *sequence*, in a *single Dataflow job*.
            subprocess.check_call(['/usr/bin/java', '-jar', args.beam_jar_path, 'import',
                                   '--runner=dataflow',
                                   '--project='+args.gcp_project,
                                   '--bigtableInstanceId='+args.bigtable_instance_id,
                                   '--bigtableTableId='+table_short_name,
                                   '--sourcePattern='+'gs://'+source_pattern,
                                   '--tempLocation='+'gs://'+args.bucket_name+'/jar-temp',
                                   '--maxNumWorkers='+node_count,
                                   '--diskSizeGb=30',
                                   '--workerMachineType=n1-standard-1',
                                   '--jobName=bt-restore-'+date+'-'+re.sub('[^-a-z0-9]', '-', table_short_name.lower()),
                                   '--region='+cluster_region])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.HelpFormatter)
    required = parser.add_argument_group('required arguments')
    required.add_argument('--beam_jar_path', help='Path to the Bigtable HBase client jar file.', required=True)
    required.add_argument('--gcp_project', help='ID of the Bigtable instance parent GCP project.', required=True)
    required.add_argument('--bigtable_instance_id', help='ID of the Bigtable instance.', required=True)
    required.add_argument('--bigtable_cluster_id', help='ID of the cluster in the Bigtable instance.', required=True)
    required.add_argument('--bucket_name', help='GCS bucket name to fetch the Bigtable dumps from.', required=True)
    required.add_argument('--backup_gcs_dir', help='Bucket directory with subdirectories containing the Hadoop sequence'
                                                   ' files to be imported.', required=True)
    parser.add_argument('--force', help='Proceed with import even if there\'s no successful backup marker blob at the '
                                        'GCS location indicated by `--backup_gcs_dir`.', required=False,
                        action='store_true')
    # required.add_argument('--auth_json_path', help='Path to the Google credentials JSON file with a service account key.', required=True)
    args = parser.parse_args()

    main()

