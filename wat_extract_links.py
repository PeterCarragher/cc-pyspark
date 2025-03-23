import idna
import os
import re

from urllib.parse import urljoin, urlparse

from pyspark.sql.types import StructType, StructField, StringType, NumericType, IntegerType, ArrayType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Row

from sparkcc import CCIndexWarcSparkJob
from collections import Counter

class ExtractLinksJob(CCIndexWarcSparkJob):
    """Extract links from WAT files and redirects from WARC files
    and save them as pairs <from, to>"""
    name = "ExtractLinks"

    output_schema = StructType([
        StructField("s", StringType(), True),
        StructField("t", StringType(), True),
        # StructField("c", IntegerType(), True),
        StructField("c_list", ArrayType(IntegerType()), True),
    ])


    warc_parse_http_header = False

    processing_robotstxt_warc = False

    records_response = None
    records_response_wat = None
    records_response_warc = None
    records_response_robotstxt = None
    records_failed = None
    records_non_html = None
    records_response_redirect = None
    link_count = None

    http_redirect_pattern = re.compile(b'^HTTP\\s*/\\s*1\\.[01]\\s*30[12378]\\b')
    http_redirect_location_pattern = re.compile(b'^Location:\\s*(\\S+)',
                                                re.IGNORECASE)
    http_link_pattern = re.compile(r'<([^>]*)>')
    http_success_pattern = re.compile(b'^HTTP\\s*/\\s*1\\.[01]\\s*200\\b')
    robotstxt_warc_path_pattern = re.compile(r'.*/robotstxt/')
    robotstxt_sitemap_pattern = re.compile(b'^Sitemap:\\s*(\\S+)',
                                           re.IGNORECASE)
    url_abs_pattern = re.compile(r'^(?:https?:)?//')
    url_href_pattern = re.compile(r'^.*?href=["\'](https?://[^"\']+)["\'].*?$')
    # num_input_partitions = 32
    # num_output_partitions = 16

    # match global links
    # - with URL scheme, more restrictive than specified in
    #   https://tools.ietf.org/html/rfc3986#section-3.1
    # - or starting with //
    #   (all other "relative" links are within the same host)
    global_link_pattern = re.compile(r'^(?:[a-z][a-z0-9]{1,5}:)?//',
                                     re.IGNORECASE|re.ASCII)

    # match IP addresses
    # - including IPs with leading `www.' (stripped)
    ip_pattern = re.compile(r'^(?:www\.)?\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\Z')

    # valid host names, relaxed allowing underscore, allowing also IDNAs
    # https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_hostnames
    host_part_pattern = re.compile(r'^[a-z0-9]([a-z0-9_-]{0,61}[a-z0-9])?\Z',
                                   re.IGNORECASE|re.ASCII)

    # simple pattern to match many but not all host names in URLs
    url_parse_host_pattern = re.compile(r'^https?://([a-z0-9_.-]{2,253})(?:[/?#]|\Z)',
                                        re.IGNORECASE|re.ASCII)

    # Meta properties usually offering links:
    #   <meta property="..." content="https://..." />
    html_meta_property_links = {
        'og:url', 'og:image', 'og:image:secure_url',
        'og:video', 'og:video:url', 'og:video:secure_url',
        'twitter:url', 'twitter:image:src'}
    # Meta names usually offering links
    html_meta_links = {
        'twitter:image', 'thumbnail', 'application-url',
        'msapplication-starturl', 'msapplication-TileImage', 'vb_meta_bburl'}

    def add_arguments(self, parser):
        super(ExtractLinksJob, self).add_arguments(parser)
        parser.add_argument("--intermediate_output", type=str,
                            default=None,
                            help="Intermediate output to recover job from")

    @staticmethod
    def _url_join(base, link):
        # TODO: efficiently join without reparsing base
        # TODO: canonicalize
        pass

    def iterate_records(self, warc_uri, archive_iterator):
        """Iterate over all WARC records and process them"""
        self.processing_robotstxt_warc \
            = ExtractLinksJob.robotstxt_warc_path_pattern.match(warc_uri)
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res
            self.records_processed.add(1)


    def analyze_url_output(self, url_gen):
        pairs = []
        for src_host, thost in url_gen:
            pairs.append((src_host, thost))

        pair_counts = Counter(pairs)
        unique_pairs_flat = [(item[0][0], item[0][1], item[1]) for item in pair_counts.items()]
        return unique_pairs_flat

    def process_url(self, stream, src):
        line = stream.readline()
        while line:
            # Decode the line if it's in bytes format
            line = line.decode('utf-8', errors='replace')  # Replace 'utf-8' with the correct encoding

            matches = ExtractLinksJob.url_href_pattern.finditer(line)
            for m in matches:
                # Check if there is a match before accessing the group
                if m.group(1) is not None:
                    url = m.group(1).strip()
                    try:
                        src_host = ExtractLinksJob.get_surt_host(src)
                        thost = ExtractLinksJob.get_surt_host(url)
                        if thost and src_host and src_host != thost:
                            yield src_host, thost
                    except UnicodeError as e:
                        self.get_logger().warning(
                            'URL with unknown encoding: {} - {}'.format(url, e))
            line = stream.readline()


    def process_record(self, record):
        self.records_response.add(1)
        self.records_response_warc.add(1)
        stream = self.get_payload_stream(record)
        uri = self.get_warc_header(record, 'WARC-Target-URI')
        url_gen = self.process_url(stream, uri)
        return self.analyze_url_output(url_gen)

    @staticmethod
    def get_surt_host(url):
        m = ExtractLinksJob.url_parse_host_pattern.match(url)
        if m:
            host = m.group(1)
        else:
            try:
                host = urlparse(url).hostname
            except Exception as e:
                # self.get_logger().debug("Failed to parse URL {}: {}\n".format(url, e))
                return None
            if not host:
                return None
        host = host.strip().lower()
        if len(host) < 1 or len(host) > 253:
            return None
        if ExtractLinksJob.ip_pattern.match(host):
            return None
        parts = host.split('.')
        if parts[-1] == '':
            # trailing dot is allowed, strip it
            parts = parts[0:-1]
        if len(parts) <= 1:
            # do not accept single-word hosts, must be at least `domain.tld'
            return None
        if len(parts) > 2 and parts[0] == 'www':
            # strip leading 'www' to reduce number of "duplicate" hosts,
            # but leave at least 2 trailing parts (www.com is a valid domain)
            parts = parts[1:]
        for (i, part) in enumerate(parts):
            if len(part) > 63:
                return None
            if not ExtractLinksJob.host_part_pattern.match(part):
                try:
                    idn = idna.encode(part).decode('ascii')
                except (idna.IDNAError, idna.core.InvalidCodepoint, UnicodeError, IndexError, Exception):
                    # self.get_logger().debug("Invalid host name: {}".format(url))
                    return None

                # TODO: idna verifies the resulting string for length restrictions or invalid chars,
                #       maybe no further verification is required:
                if ExtractLinksJob.host_part_pattern.match(idn):
                    parts[i] = idn
                else:
                    # self.get_logger().debug("Invalid host name: {}".format(url))
                    return None
        parts.reverse()
        return '.'.join(parts)

    def init_accumulators(self, session):
        super(ExtractLinksJob, self).init_accumulators(session)

        sc = session.sparkContext
        self.records_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)
        self.records_response = sc.accumulator(0)
        self.records_response_wat = sc.accumulator(0)
        self.records_response_warc = sc.accumulator(0)
        self.records_response_redirect = sc.accumulator(0)
        self.records_response_robotstxt = sc.accumulator(0)
        self.link_count = sc.accumulator(0)

    def log_accumulators(self, session):
        super(ExtractLinksJob, self).log_accumulators(session)

        self.log_accumulator(session, self.records_response,
                             'response records = {}')
        self.log_accumulator(session, self.records_failed,
                             'records failed to process = {}')
        self.log_accumulator(session, self.records_non_html,
                             'records not HTML = {}')
        self.log_accumulator(session, self.records_response_wat,
                             'response records WAT = {}')
        self.log_accumulator(session, self.records_response_warc,
                             'response records WARC = {}')
        self.log_accumulator(session, self.records_response_redirect,
                             'response records redirects = {}')
        self.log_accumulator(session, self.records_response_robotstxt,
                             'response records robots.txt = {}')
        self.log_accumulator(session, self.link_count,
                             'non-unique link pairs = {}')


    # def load_checkpoint(self, checkpoint_dir, session):
    #     try:
    #         return int(session.sparkContext.getCheckpointDir().split("_")[-1])
    #     except:
    #         return 0

    # def save_checkpoint(self, checkpoint_dir, idx, session):
    #     session.sparkContext.setCheckpointDir(checkpoint_dir)
    #     session.sparkContext.parallelize([idx]).saveAsTextFile(f"{checkpoint_dir}/checkpoint_{idx}")

    # def run_job(self, session):
    #     output = None
    #     # session.conf.set("spark.executor.cores", "16")
        
    #     if self.args.input != '':
    #         input_data = session.sparkContext.textFile(
    #             self.args.input,
    #             minPartitions=self.args.num_input_partitions)
    #         checkpoint_dir = "file:///home/pcarragh/dev/cc/cc-pyspark/output/checkpoints"
    #         session.sparkContext.setCheckpointDir(checkpoint_dir)

    #         # Load the last successfully processed index
    #         last_processed_index = self.load_checkpoint(checkpoint_dir, session)

    #         input_data = input_data.coalesce(18)

    #         filtered_partitions = input_data.zipWithIndex().filter(lambda x: x[1] > last_processed_index).map(lambda x: x[0])
    #         output_rdds = filtered_partitions.mapPartitions(self.process_warcs)
    #         output_rdds.saveAsTextFile("output/intermediate/full_p1")


    #     self.log_accumulators(session.sparkContext)

    def run_job(self, session):
        sqldf = self.load_dataframe(session, self.args.num_input_partitions)

        columns = ['url', 'warc_filename', 'warc_record_offset', 'warc_record_length']
        if 'content_charset' in sqldf.columns:
            columns.append('content_charset')
        warc_recs = sqldf.select(*columns).rdd

        output = warc_recs.mapPartitions(self.fetch_process_warc_records)
        grouped_output = output.groupBy(lambda x: (x[0], x[1]))

        # Transform the grouped data using Row
        transformed_output = grouped_output.map(lambda group: Row(s=group[0][0], t=group[0][1], c_list=[item[2] for item in group[1]]))

            # .groupBy('s', 't').agg(F.collect_list('c_list').alias('c_list')) \

        session.createDataFrame(transformed_output, schema=self.output_schema) \
            .coalesce(self.args.num_output_partitions) \
            .write \
            .format(self.args.output_format) \
            .option("compression", self.args.output_compression) \
            .options(**self.get_output_options()) \
            .saveAsTable(self.args.output)

        self.log_accumulators(session)


if __name__ == "__main__":
    job = ExtractLinksJob()
    job.run()


# df = sqlContext.read.parquet("spark-warehouse/extracted_links")
# df.createOrReplaceTempView("test_df")
# spark.sql("""SELECT s, t, COUNT(*) AS pair_count FROM test_df GROUP BY s, t ORDER BY pair_count DESC""").show(10)
# spark.sql("""SELECT COUNT(DISTINCT CONCAT(s, '|', t)) AS distinct_pairs_count FROM test_df""").show(10)
# spark.sql("""SELECT COUNT(*) AS count FROM test_df""").show(10)
# spark.sql("""SELECT s,t,size(c_list) as num_pages,aggregate(c_list, 0, (x, y) -> x + y) as details_sum FROM test_df ORDER BY num_pages DESC""").show(100)
