"""Count weighted domain-level links between a fixed list of news domains.

Fetches WARC response records via the CC columnar index for pages belonging
to domains in --target_domains, parses HTML for <a href> links, and counts
all link occurrences per (source_domain, target_domain) pair where BOTH the
source and target are in the domain list.

This is a simplified version of count_domain_links.py for the specific use
case of computing a link-weight matrix within a closed domain set (e.g. news
outlets linking to each other).

Output schema: s STRING, t STRING, nav_count LONG, body_count LONG
  s, t        -- registered domains (eTLD+1), e.g. 'bbc.co.uk'
  nav_count   -- links from s to t found inside <header>, <footer>, <nav>,
                 <aside>, or elements whose class/id suggests navigation
  body_count  -- links from s to t found inside <article>, <main>, or
                 elements whose class/id suggests article body content.
                 Uncontextualized links default to body.

Usage:
  # Run on EMR Serverless (expanded-domains mode):
  bash run_ccpyspark_job_aws_emr.sh \\
    s3://bucket/code/count_news_links.py \\
    s3://bucket/warehouse \\
    s3://commoncrawl/cc-index/table/cc-main/warc/ \\
    news_links \\
    --input_base_url s3://commoncrawl/ \\
    --target_domains s3://bucket/target_domains.csv \\
    --crawl CC-MAIN-2024-38 \\
    --num_input_partitions 200 \\
    --num_output_partitions 5
"""

import tldextract

# Use the bundled Public Suffix List; disables network fetching which would
# fail in air-gapped environments such as EMR Serverless.
_tld_extract = tldextract.TLDExtract(suffix_list_urls=())

from collections import Counter
from html.parser import HTMLParser
from urllib.parse import urljoin

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from sparkcc import CCIndexWarcSparkJob, CCSparkJob


def _registered_domain(url_or_domain):
    """Return the registered domain (eTLD+1) for a URL or bare domain, or None."""
    ext = _tld_extract(url_or_domain)
    if not ext.domain or not ext.suffix:
        return None
    return '{}.{}'.format(ext.domain, ext.suffix)


class CountNewsLinksJob(CCIndexWarcSparkJob):
    """Count weighted links between a fixed set of domains (e.g. news outlets).

    Only fetches WARCs for domains in --target_domains. Only counts links
    whose target is also in --target_domains. Self-links (s == t) are dropped.
    """

    name = 'CountNewsLinks'

    output_schema = StructType([
        StructField('s', StringType(), True),
        StructField('t', StringType(), True),
        StructField('nav_count', LongType(), True),
        StructField('body_count', LongType(), True),
    ])

    warc_parse_http_header = True

    # Broadcast set of target domains (plain registered-domain strings)
    target_domains_bc = None

    # Accumulators
    records_html = None
    records_non_html = None
    records_failed = None
    link_count = None

    # ── HTML parsing ───────────────────────────────────────────────────────────

    # Void elements never have a closing tag; don't push them onto the stack.
    _VOID_ELEMENTS = frozenset([
        'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input',
        'link', 'meta', 'param', 'source', 'track', 'wbr',
    ])

    # Tags that are unambiguously navigational/structural chrome.
    _NAV_TAGS = frozenset(['header', 'footer', 'nav', 'aside'])

    # Tags that are unambiguously article body.
    # <p> is included: editorial citations are almost always inline prose;
    # nav/footer links are almost never wrapped in <p> tags.
    _BODY_TAGS = frozenset(['article', 'main', 'p'])

    # class/id substrings that suggest navigational context.
    _NAV_HINTS = frozenset([
        'nav', 'menu', 'header', 'footer', 'sidebar', 'breadcrumb',
        'share', 'social', 'related', 'widget', 'toolbar', 'banner',
        'masthead', 'topbar', 'subnav', 'utility',
    ])

    # class/id substrings that suggest editorial body context.
    _BODY_HINTS = frozenset([
        'article', 'story', 'post', 'entry', 'content', 'body',
        'prose', 'text', 'detail', 'richtext', 'byline', 'copy',
    ])

    @classmethod
    def _classify_tag(cls, tag, attrs_d):
        """Return 'nav', 'body', or None (inherit from parent) for an opening tag."""
        if tag in cls._NAV_TAGS:
            return 'nav'
        if tag in cls._BODY_TAGS:
            return 'body'
        # Inspect class and id attributes for hint words
        class_id = (attrs_d.get('class', '') + ' ' + attrs_d.get('id', '')).lower()
        if any(h in class_id for h in cls._NAV_HINTS):
            return 'nav'
        if any(h in class_id for h in cls._BODY_HINTS):
            return 'body'
        return None  # no signal; inherit from ancestor

    class _LinkParser(HTMLParser):
        """SAX-style parser that classifies each <a href> as nav or body.

        Maintains a stack of (tag, context) pairs where context is 'nav',
        'body', or None (inherit). Each <a> is classified by walking the
        stack to find the nearest explicit ancestor context. Uncontextualized
        links default to 'body' (most page content is body by default).
        """

        def __init__(self, classify_tag_fn, void_elements):
            super(CountNewsLinksJob._LinkParser, self).__init__()
            self._classify_tag = classify_tag_fn
            self._void_elements = void_elements
            self.base = None
            self.nav_hrefs = []
            self.body_hrefs = []
            self._stack = []  # list of (tag, context)  context='nav'|'body'|None

        def _current_context(self):
            for _, ctx in reversed(self._stack):
                if ctx is not None:
                    return ctx
            return 'nav'  # default: uncontextualized links are nav

        def handle_starttag(self, tag, attrs):
            attrs_d = dict(attrs)
            if tag == 'base' and self.base is None:
                self.base = attrs_d.get('href')

            if tag == 'a':
                href = attrs_d.get('href')
                if href:
                    if self._current_context() == 'nav':
                        self.nav_hrefs.append(href)
                    else:
                        self.body_hrefs.append(href)

            if tag not in self._void_elements:
                ctx = self._classify_tag(tag, attrs_d)
                self._stack.append((tag, ctx))

        def handle_endtag(self, tag):
            # Pop back to the most recent matching open tag, tolerating
            # malformed HTML where tags may be unmatched or misnested.
            for i in range(len(self._stack) - 1, -1, -1):
                if self._stack[i][0] == tag:
                    self._stack = self._stack[:i]
                    break

        def error(self, _message):
            pass

    def _get_link_counts(self, url, html_bytes, src_domain):
        """Parse HTML; return (nav_counts, body_counts) Counters for in-list targets."""
        target_set = self.target_domains_bc.value
        nav_counts = Counter()
        body_counts = Counter()
        try:
            parser = self._LinkParser(self._classify_tag, self._VOID_ELEMENTS)
            parser.feed(html_bytes.decode('utf-8', errors='replace'))
            base_url = url
            if parser.base:
                try:
                    base_url = urljoin(url, parser.base)
                except Exception:
                    pass
            for href_list, counts in (
                (parser.nav_hrefs, nav_counts),
                (parser.body_hrefs, body_counts),
            ):
                for href in href_list:
                    try:
                        abs_url = urljoin(base_url, href)
                        tdomain = _registered_domain(abs_url)
                    except Exception:
                        continue
                    if tdomain and tdomain != src_domain and tdomain in target_set:
                        counts[tdomain] += 1
        except Exception:
            pass
        return nav_counts, body_counts

    # ── WARC record processing ─────────────────────────────────────────────────

    def process_record(self, record):
        if not self.is_response_record(record):
            return
        if record.http_headers.get_statuscode() != '200':
            return
        ct = record.http_headers.get_header('Content-Type', '')
        if 'text/html' not in ct.lower():
            self.records_non_html.add(1)
            return
        url = record.rec_headers.get_header('WARC-Target-URI')
        if not url:
            return
        src_domain = _registered_domain(url)
        if not src_domain or src_domain not in self.target_domains_bc.value:
            return
        try:
            html_bytes = self.get_payload_stream(record).read()
        except Exception as e:
            self.get_logger().error('Failed to read {}: {}'.format(url, e))
            self.records_failed.add(1)
            return
        self.records_html.add(1)
        nav_counts, body_counts = self._get_link_counts(url, html_bytes, src_domain)
        all_targets = set(nav_counts) | set(body_counts)
        for tdomain in all_targets:
            n = nav_counts.get(tdomain, 0)
            b = body_counts.get(tdomain, 0)
            self.link_count.add(n + b)
            yield src_domain, tdomain, n, b

    # ── Accumulators ───────────────────────────────────────────────────────────

    def init_accumulators(self, session):
        super(CountNewsLinksJob, self).init_accumulators(session)
        sc = session.sparkContext
        self.records_html = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)
        self.records_failed = sc.accumulator(0)
        self.link_count = sc.accumulator(0)

    def log_accumulators(self, session):
        super(CountNewsLinksJob, self).log_accumulators(session)
        self.log_accumulator(session, self.records_html,
                             'HTML records processed = {}')
        self.log_accumulator(session, self.records_non_html,
                             'non-HTML records skipped = {}')
        self.log_accumulator(session, self.records_failed,
                             'records failed = {}')
        self.log_accumulator(session, self.link_count,
                             'total in-list links counted = {}')

    # ── Arguments ──────────────────────────────────────────────────────────────

    def add_arguments(self, parser):
        CCSparkJob.add_arguments(self, parser)

        parser.add_argument(
            '--target_domains', required=True,
            help='Path to CSV of domains (one per line or with a header). '
                 'Values are cleaned with domain.split("/")[0] and deduplicated. '
                 'WARCs are fetched only for these domains; only links between '
                 'domains in this list are counted.')
        parser.add_argument(
            '--crawl', required=True,
            help='CC-MAIN crawl ID to filter the columnar index, '
                 'e.g. CC-MAIN-2024-38.')

        # Forwarded to CCIndexWarcSparkJob internals
        parser.add_argument('--input_table_format', default=None,
                            help='Set to "parquet" to read a pre-built WARC '
                                 'index table instead of the CC columnar index.')
        parser.add_argument('--query', default=None)
        parser.add_argument('--csv', default=None)
        parser.add_argument('--input_table_option', action='append', default=[])

    # ── Setup ──────────────────────────────────────────────────────────────────

    def _load_target_domains(self, session):
        """Read, clean, deduplicate, and broadcast --target_domains."""
        raw = session.read.csv(self.args.target_domains, header=False)
        first_col = raw.columns[0]
        domains = set()
        for row in raw.select(first_col).collect():
            val = row[0]
            if not val:
                continue
            # Strip path component and normalise
            val = val.split('/')[0].strip().lower()
            if not val or val == 'website' or val == 'domain':
                continue
            domain = _registered_domain(val)
            if domain:
                domains.add(domain)
        self.target_domains_bc = session.sparkContext.broadcast(domains)
        self.get_logger().info(
            'Target domains loaded: {}'.format(len(domains)))
        return domains

    def _query_cc_index(self, session):
        """Query the CC columnar index for HTML pages in the target domain set."""
        domains = self.target_domains_bc.value
        domain_df = session.createDataFrame(
            [(d,) for d in domains],
            StructType([StructField('host', StringType(), True)])
        )
        result = (
            session.read.parquet(self.args.input)
            .filter(F.col('crawl') == self.args.crawl)
            .filter(F.col('subset') == 'warc')
            .filter(F.col('fetch_status') == 200)
            .filter(F.col('content_mime_detected') == 'text/html')
            .join(F.broadcast(domain_df),
                  F.col('url_host_registered_domain') == domain_df['host'],
                  how='inner')
            .select('url', 'warc_filename', 'warc_record_offset', 'warc_record_length')
        )
        if self.args.num_input_partitions > 0:
            result = result.repartition(self.args.num_input_partitions)
        result.persist()
        n = result.count()
        self.get_logger().info('CC index records selected: {}'.format(n))
        return result

    # ── Job orchestration ───────────────────────────────────────────────────────

    def run_job(self, session):
        self._load_target_domains(session)
        index_df = self._query_cc_index(session)

        warc_recs = index_df.select(
            'url', 'warc_filename', 'warc_record_offset', 'warc_record_length'
        ).rdd

        output = warc_recs.mapPartitions(self.fetch_process_warc_records)

        (
            session.createDataFrame(output, schema=self.output_schema)
            .dropna(subset=['s', 't'])
            .filter(F.col('s') != F.col('t'))
            .groupBy('s', 't')
            .agg(
                F.sum('nav_count').alias('nav_count'),
                F.sum('body_count').alias('body_count'),
            )
            .coalesce(self.args.num_output_partitions)
            .write
            .format(self.args.output_format)
            .option('compression', self.args.output_compression)
            .options(**self.get_output_options())
            .saveAsTable(self.args.output)
        )

        self.log_accumulators(session)


if __name__ == '__main__':
    job = CountNewsLinksJob()
    job.run()
