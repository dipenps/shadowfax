# Shadowfax: One NGS workhorse to rule them all.
# Author: Dipen Sangurdekar
#------------------------------

import argparse
import sys
from configobj import *
from SFclasses import *
from ngsutils import *
from tabulate import tabulate


def main(ROOTPATH=None):
    #####################
    # Get arguments
    parser = argparse.ArgumentParser(description='One NGS workhorse to rule them all', prog='shadowfax')
    parser.add_argument('--series', help='Run pipelines in series?', action='store_true', default=False)
    parser.add_argument('--pipeline', help='Which pipeline?', default=None)
    parser.add_argument('--config', help='Configfile', required=False)
    #parser.add_argument('--globalconfig', help='Global Configfile (Optional)', default=None)
    parser.add_argument('--test', help='If used, only Pipeline commands printed. No jobs submitted',
                        action='store_true', default=False)
    parser.add_argument('--show', help='Show detailed description about pipelines', action='store_true', default=False)
    parser.add_argument('--version', help='Show version and exit', action='store_true', default=False)
    args = parser.parse_args()

    #####################
    # Show version
    version = 'v0.1' #SETVERSION
    print 'Shadowfax version {0}'.format(version)
    if args.version: sys.exit(0)

    #####################
    # Help
    if args.show:
        outstr = '#'*80 + '\n'
        outstr += """
        |                 |                    _|
   __|  __ \    _` |   _` |   _ \ \ \  \   /  |     _` | \ \  /
 \__ \  | | |  (   |  (   |  (   | \ \  \ /   __|  (   |  `  <
 ____/ _| |_| \__,_| \__,_| \___/   \_/\_/   _|   \__,_|  _/\_\
        \n"""

        outstr += 'Shadowfax - one NGS workhorse to rule them all. Version {0} \n'.format(version)
        outstr += 'Author: Dipen Sangurdekar \n'
        outstr += '#'*80 + '\n\n'

        outstr += 'Basic Usage:\n'
        outstr += '-'*45 + '\n'
        outstr += '$ module use sge, shadowfax\n'
        outstr += '$ shadowfax.py --help\n'
        outstr += '$ shadowfax.py --config config_file.txt --series\n\n'

        outstr += '-'*45 + '\n'
        outstr += 'Available pipelines by type of application'
        table = tabulate([
            ['Application', 'Pipeline', 'Tasks', 'Notes'],
            ['RNA-seq (gene/isoform, counts, FPKM):', 'Prego', 'map/count/fqc', 'STAR/RSEM/RNA-SeqQC'],
        ])

        outstr2 = '\nNote: For all pipelines, use flag_xenome = yes to filter out PDX reads\n'
        print outstr
        print table
        print outstr2

        sys.exit(0)

    pset = get_config('__file__/pipelines.txt')
    config = get_config(args.config)

    #####################
    # Check pipeline
    if args.pipeline is not None:
        args.pipeline = args.pipeline.lower().strip()
    else:
        args.pipeline = config['Pipeline_Info']['pipeline'].lower()

    #####################
    # Run in series mode
    if args.pipeline.lower() not in pset['pipelines'].keys():
        print 'Pipeline {0} not one of supported pipelines {1}'.format(args.pipeline, ','.join(pset['pipelines'].keys()))
    if args.series:
        Herd(args=args, rootpath=ROOTPATH,
                    pipelineset=pset['pipelines'])
        sys.exit(0)

    #####################
    # Run in pipeline mode
    #ADDPIPELINE
    if args.pipeline is not None and not args.series:
        if args.pipeline == 'prego':
            print 'Running Prego'
            Prego(args=args, confpath=ROOTPATH)


if __name__ == '__main__':
    main(os.path.dirname(__file__))
