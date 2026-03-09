#!/usr/bin/env python3
"""Expand a list of target domains to their full backlinker set using pyccwebgraph.

Runs locally (no Spark required). Downloads the CC domain graph on first use
(~30 GB disk). Outputs a CSV of all expanded domains (seeds + backlinkers)
suitable for passing to count_domain_links.py via --target_domains without
--webgraph_prefix, so the Spark job queries the CC index directly against the
pre-expanded set without repeating the graph traversal.

Usage:
    python expand_domains_local.py \\
        --seeds domains.csv \\
        --output expanded_domains.csv \\
        [--version cc-main-2024-aug-sep-oct] \\
        [--webgraph_dir /data/webgraph]

    # Upload and run the Spark job (no --webgraph_prefix needed):
    aws s3 cp expanded_domains.csv s3://my-bucket/expanded_domains.csv
    bash run_ccpyspark_job_aws_emr.sh \\
        s3://my-bucket/code/count_domain_links.py \\
        s3://my-bucket/warehouse \\
        s3://commoncrawl/cc-index/table/cc-main/warc/ \\
        domain_links \\
        --input_base_url s3://commoncrawl/ \\
        --target_domains s3://my-bucket/expanded_domains.csv \\
        --crawl CC-MAIN-2024-38 \\
        --num_input_partitions 500 \\
        --num_output_partitions 20

Requirements:
    pip install pyccwebgraph tldextract
    java 17+
"""

import argparse
import csv
import sys

import tldextract

_tld_extract = tldextract.TLDExtract(suffix_list_urls=())


def _registered_domain(val):
    """Return eTLD+1 (e.g. 'mirror.co.uk') for a URL or bare domain, or None."""
    ext = _tld_extract(val.strip())
    if not ext.domain or not ext.suffix:
        return None
    return '{}.{}'.format(ext.domain, ext.suffix)


def load_seeds(csv_path):
    """Load and deduplicate seed domains from a CSV (one per line or 'domain' header)."""
    seeds = []
    seen = set()
    with open(csv_path) as f:
        for row in csv.reader(f):
            if not row:
                continue
            val = row[0].strip()
            if not val or val.lower() == 'domain':
                continue
            domain = _registered_domain(val)
            if domain and domain not in seen:
                seeds.append(domain)
                seen.add(domain)
    return seeds


def main():
    parser = argparse.ArgumentParser(
        description='Expand target domains to their full backlinker set via CC domain graph.')
    parser.add_argument('--seeds', required=True,
                        help='CSV of seed (target) domains, one per line or with a '
                             '"domain" header column.')
    parser.add_argument('--output', required=True,
                        help='Output CSV path for the expanded domain list '
                             '(seeds + all backlinkers).')
    parser.add_argument('--version', default='cc-main-2024-aug-sep-oct',
                        help='CC webgraph version string '
                             '(default: cc-main-2024-aug-sep-oct). '
                             'Use pyccwebgraph.get_available_versions() to list options.')
    parser.add_argument('--webgraph_dir', default=None,
                        help='Local directory for graph data '
                             '(default: ~/.pyccwebgraph/data). '
                             'Data is downloaded automatically on first use (~30 GB).')
    args = parser.parse_args()

    seeds = load_seeds(args.seeds)
    if not seeds:
        print('ERROR: no valid seed domains found in {}'.format(args.seeds),
              file=sys.stderr)
        sys.exit(1)
    print('Loaded {} seed domains: {}'.format(len(seeds), seeds))

    from pyccwebgraph import CCWebgraph
    webgraph = CCWebgraph.setup(
        webgraph_dir=args.webgraph_dir,
        version=args.version,
    )

    found, missing = webgraph.validate_seeds(seeds)
    if missing:
        print('WARNING: {} seed(s) not found in graph: {}'.format(
            len(missing), missing))
    if not found:
        print('ERROR: none of the seeds were found in the graph', file=sys.stderr)
        sys.exit(1)

    # discover_backlinks: for each seed, finds all domains that link TO it.
    # min_connections=1 means: include any domain that links to at least 1 seed.
    result = webgraph.discover_backlinks(seeds=found, min_connections=1)

    backlinker_domains = {node['domain'] for node in result.nodes}
    seed_set = set(found)
    all_domains = seed_set | backlinker_domains

    print('Seeds found in graph: {}'.format(len(seed_set)))
    print('Backlinker domains:   {}'.format(len(backlinker_domains)))
    print('Total expanded set:   {}'.format(len(all_domains)))

    with open(args.output, 'w', newline='') as f:
        writer = csv.writer(f)
        for domain in sorted(all_domains):
            writer.writerow([domain])

    print('Expanded domain list written to: {}'.format(args.output))


if __name__ == '__main__':
    main()
