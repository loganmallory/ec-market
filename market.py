import boto3
import concurrent.futures
import datetime
import numpy as np
import pandas as pd
import pytz
import queue
import requests
import threading
import time
import tqdm
import urllib.parse

DEBUG = False

################################################################################
# 1. Market data scraper
# 2. Flask server
# e.g. python3 market.py --browser
################################################################################


################################################################################
# Market data scraper
################################################################################

def paginate_boto3(method, **kwargs):
    client = method.__self__
    paginator = client.get_paginator(method.__name__)
    for page in paginator.paginate(**kwargs).result_key_iters():
        for item in page:
            yield item


def get_aws_regions(local_zones=False):
    if DEBUG: print(f'Getting aws regions (local_zones={local_zones}) ...')
    # {"Africa (Cape Town)":{"name":"Africa (Cape Town)","code":"af-south-1","type":"AWS Region","label":"Africa (Cape Town)","continent":"Africa"},"Asia Pacific (Hong Kong)":{"name":"Asia Pacific (Hong Kong)","code":"ap-east-1","type":"AWS Region","label":"Asia Pacific (Hong Kong)","continent":"Asia Pacific"}, ...}
    r = requests.get('https://b0.p.awsstatic.com/locations/1.0/aws/current/locations.json', timeout=5)
    if r.status_code != 200:
        print(f'get_aws_regions(): Bad response: {r.content.decode("utf-8")}')
    if local_zones:
        return [v for (k, v) in r.json().items()]
    else:
        return [v for (k, v) in r.json().items() if v['type'] == 'AWS Region' and v['code'].count('-') <= 2]


def get_aws_ec2_instance_types(boto3_client=None):
    if DEBUG: print('Getting ec2 instance types ...')
    # {'InstanceType': 'r7a.2xlarge', 'CurrentGeneration': True, 'FreeTierEligible': False, 'SupportedUsageClasses': ['on-demand', 'spot'], 'SupportedRootDeviceTypes': ['ebs'], 'SupportedVirtualizationTypes': ['hvm'], 'BareMetal': False, 'Hypervisor': 'nitro', 'ProcessorInfo': {'SupportedArchitectures': ['x86_64'], 'SustainedClockSpeedInGhz': 3.7}, 'VCpuInfo': {'DefaultVCpus': 8, 'DefaultCores': 8, 'DefaultThreadsPerCore': 1, 'ValidCores': [1, 2, 3, 4, 5, 6, 7, 8], 'ValidThreadsPerCore': [1]}, 'MemoryInfo': {'SizeInMiB': 65536}, 'InstanceStorageSupported': False, 'EbsInfo': {'EbsOptimizedSupport': 'default', 'EncryptionSupport': 'supported', 'EbsOptimizedInfo': {'BaselineBandwidthInMbps': 2500, 'BaselineThroughputInMBps': 312.5, 'BaselineIops': 12000, 'MaximumBandwidthInMbps': 10000, 'MaximumThroughputInMBps': 1250.0, 'MaximumIops': 40000}, 'NvmeSupport': 'required'}, 'NetworkInfo': {'NetworkPerformance': 'Up to 12.5 Gigabit', 'MaximumNetworkInterfaces': 4, 'MaximumNetworkCards': 1, 'DefaultNetworkCardIndex': 0, 'NetworkCards': [{'NetworkCardIndex': 0, 'NetworkPerformance': 'Up to 12.5 Gigabit', 'MaximumNetworkInterfaces': 4, 'BaselineBandwidthInGbps': 3.125, 'PeakBandwidthInGbps': 12.5}], 'Ipv4AddressesPerInterface': 15, 'Ipv6AddressesPerInterface': 15, 'Ipv6Supported': True, 'EnaSupport': 'required', 'EfaSupported': False, 'EncryptionInTransitSupported': True, 'EnaSrdSupported': False}, 'PlacementGroupInfo': {'SupportedStrategies': ['cluster', 'partition', 'spread']}, 'HibernationSupported': True, 'BurstablePerformanceSupported': False, 'DedicatedHostsSupported': True, 'AutoRecoverySupported': True, 'SupportedBootModes': ['legacy-bios', 'uefi'], 'NitroEnclavesSupport': 'unsupported', 'NitroTpmSupport': 'supported', 'NitroTpmInfo': {'SupportedVersions': ['2.0']}}
    if boto3_client is None:
        boto3_client = boto3.client('ec2', region_name='us-west-2')
    records = []
    for item in paginate_boto3(boto3_client.describe_instance_types):
        try:
            records.append({
                'instance_type': item['InstanceType'],
                'cpu_arch': 'x86_64' if 'x86_64' in item['ProcessorInfo']['SupportedArchitectures'] else 'arm64',
                'clock_speed_ghz': item['ProcessorInfo']['SustainedClockSpeedInGhz'] if 'SustainedClockSpeedInGhz' in item['ProcessorInfo'] else np.nan,
                'hyperthreaded': int(item['VCpuInfo']['DefaultThreadsPerCore']) > 1 ,
                'n_vcpu': item['VCpuInfo']['DefaultVCpus'],
                'n_cores': item['VCpuInfo']['DefaultCores'],
                'memory_gb': round(item['MemoryInfo']['SizeInMiB'] / 953.7, 2)
            })
        except Exception as e:
            input(f'{item}: {e}')

    return records


def get_aws_ec2_on_demand_prices(region_name: str, results_queue=None):
    if DEBUG: print(f'Getting ec2 on-demand prices (region={region_name}) ...')
    region_name_encoded = urllib.parse.quote(region_name)
    timestamp = time.time_ns()
    url = f'https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/{region_name_encoded}/Linux/index.json?timestamp={int(timestamp/1e6)}'
    r = requests.get(url, timeout=5)
    if r.status_code != 200:
        print(f'get_aws_ec2_on_demand_prices(region={region_name}, url={url}): Bad response: {r.content.decode("utf-8")}')
    data = [{
        'timestamp': timestamp,
        'market': 'on-demand',
        'region_name': v['Location'],
        'instance_type': v['Instance Type'],
        #'instance_os': 'linux' if v['Operating System'] == 'Linux' else v['Operating System'],
        'price_per_hr': float(v['price']),
    } for (k, v) in r.json()['regions'][region_name].items()]
    if results_queue:
        results_queue.put(data)
    return data


def get_aws_ec2_spot_interruption_data():
    if DEBUG: print(f'Getting ec2 spot interruption rates ...')
    r = requests.get('https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json', timeout=5)
    if r.status_code != 200:
        print(f'get_aws_ec2_spot_interruption_data(): Bad response: {r.content.decode("utf-8")}')
    j = r.json()

    ranges = sorted(j['ranges'], key=lambda x: x['index'])
    prev_max = 0
    for i in range(len(ranges)):
        ranges[i]        = {'min': prev_max, 'max': ranges[i]['max']}
        ranges[i]['avg'] = (ranges[i]['min'] + ranges[i]['max']) / 2.0
        prev_max         = ranges[i]['max']

    records = []
    for region_code, os_data in j['spot_advisor'].items():
        if 'Linux' not in os_data:
            print(f'Region {region_code} does not support linux!')
            continue
        os_data = os_data['Linux']
        for instance_type, data in os_data.items():
            records.append({
                'region_code': region_code,
                'instance_type': instance_type,
                'spot_interruption_rate': ranges[data['r']]['avg']
            })
    return records


def get_aws_ec2_spot_prices_history(region_code: str="", start_datetime=None, end_datetime=None, boto3_client=None):
    if DEBUG: print(f'Getting ec2 spot prices (region={region_code}, start={start_datetime}, end={end_datetime}) ...')
    if boto3_client is None:
        boto3_client = boto3.client('ec2', region_name=region_code)

    if start_datetime.tzinfo is None:
        start_datetime = start_datetime.replace(tzinfo=pytz.UTC)

    if end_datetime is None:
        end_datetime = datetime.datetime.now()

    records = []
    timestamp = time.time_ns()
    for item in paginate_boto3(
            boto3_client.describe_spot_price_history,
            InstanceTypes=[],
            ProductDescriptions=['Linux/UNIX (Amazon VPC)'],
            StartTime=start_datetime,
            EndTime=end_datetime,
            MaxResults=100000,
    ):
        # if trim and item["Timestamp"].astimezone(start_datetime.tzinfo) >= start_datetime:
        #     continue
        records.append({
            'timestamp': timestamp,
            'market': 'spot',
            'region_code': '-'.join(item['AvailabilityZone'].split('-')[:-1]) + '-' + item['AvailabilityZone'].split('-')[-1][0],
            'availability_zone_code': item['AvailabilityZone'],
            'instance_type': item['InstanceType'],
            #'instance_os': 'linux' if item['ProductDescription'] == 'Linux/UNIX' else item['ProductDescription'],
            'price_per_hr': float(item['SpotPrice']),
        })
    return records


def get_aws_ec2_spot_prices(region_code: str, results_queue=None):
    try:
        data = get_aws_ec2_spot_prices_history(region_code, datetime.datetime.now())
    except:
        if DEBUG: print(f'get_aws_ec2_spot_prices(region={region_code}): Spot price history api unavailable in this region')
        data = []
    if results_queue:
        results_queue.put(data)
    return data


def spawn_get_data_tasks(regions, markets: [str], results_queue):
    with concurrent.futures.ThreadPoolExecutor(max_workers=128) as executor:
        for region in regions:
            if 'on-demand' in markets:
                executor.submit(get_aws_ec2_on_demand_prices, region['region_name'], results_queue)
            if 'spot' in markets:
                executor.submit(get_aws_ec2_spot_prices, region['region_code'], results_queue)


def get_aws_ec2_prices_df(region_codes: [str]=None, markets=['spot', 'on-demand']):
    if DEBUG: print(f'Getting ec2 prices (regions={region_codes}, markets={markets}) ...')

    # get aws regions
    print('Getting AWS region definitions ...')
    regions_df = pd.DataFrame(get_aws_regions()).loc[:,['name', 'code']].add_prefix('region_')
    if region_codes:
        regions = [x for x in regions_df.to_dict(orient='records') if x['region_code'] in region_codes]
    else:
        regions = regions_df.to_dict(orient='records')
    print('Finished getting AWS region definitions')
    if len(regions) == 0:
        return pd.DataFrame()

    # spawn tasks to get data
    results_queue = queue.Queue()
    submission_thread = threading.Thread(target=spawn_get_data_tasks, args=(regions, markets, results_queue))
    submission_thread.start()

    # wait for data tasks to finish
    print(f'Getting EC2 market prices: {markets} ...')
    market_prices = {'on-demand': [], 'spot': []}
    total_iter_count = len(regions) * len(markets)
    progress_bar = tqdm.tqdm(total=total_iter_count)
    i = 0
    while i < total_iter_count:
        data = results_queue.get()
        i += 1
        if len(data) > 0:
            market_prices[data[0]['market']] += data
        progress_bar.update(1)
    progress_bar.close()
    print('Finished getting EC2 market prices')
    on_demand_prices_df = pd.DataFrame(market_prices['on-demand'])
    spot_prices_df = pd.DataFrame(market_prices['spot'])

    if len(spot_prices_df) > 0:
        print('Getting spot instance interruption rates ...')
        interruption_rates_df = pd.DataFrame(get_aws_ec2_spot_interruption_data())
        print('Finished getting spot instance interruption rates')
        spot_prices_df = pd.merge(spot_prices_df, interruption_rates_df, how='left', on=['region_code', 'instance_type'])

    on_demand_prices_df = pd.merge(on_demand_prices_df, regions_df, how='left', on='region_name').drop(['region_name'], axis=1)

    all_prices_df = pd.concat([on_demand_prices_df, spot_prices_df])

    instance_types_df = pd.DataFrame(get_aws_ec2_instance_types())
    instance_types_df['throttled'] = instance_types_df['instance_type'].apply(lambda x: x.split('.')[0] in ['t1', 't2','t3', 't3a', 't4g'])
    all_prices_df = pd.merge(all_prices_df, instance_types_df, how='left', on='instance_type')

    all_prices_df['timestamp'] = pd.to_datetime(all_prices_df['timestamp'], unit='ns')
    all_prices_df['timestamp'] = all_prices_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M PT')
    all_prices_df['price_per_hr_per_core']   = all_prices_df['price_per_hr'] / all_prices_df['n_cores']
    all_prices_df['price_per_hr_per_vcpu']   = all_prices_df['price_per_hr'] / all_prices_df['n_vcpu']
    all_prices_df['price_per_hr_per_mem_gb'] = all_prices_df['price_per_hr'] / all_prices_df['memory_gb']
    all_prices_df['price_per_second']        = all_prices_df['price_per_hr'] / (60 * 60)
    all_prices_df['price_per_minute']        = all_prices_df['price_per_hr'] / 60
    all_prices_df['price_per_day']           = all_prices_df['price_per_hr'] * 24
    all_prices_df['price_per_month']         = all_prices_df['price_per_hr'] * 24 * 30

    cols = ['timestamp', 'region_code', 'market', 'instance_type', 'cpu_arch', 'price_per_hr', 'n_vcpu', 'n_cores',
            'memory_gb', 'price_per_hr_per_core', 'price_per_hr_per_vcpu', 'price_per_day', 'price_per_month',
            'hyperthreaded', 'throttled', 'clock_speed_ghz', 'spot_interruption_rate']
    print(all_prices_df.columns)
    cols += [c for c in all_prices_df.columns if c not in cols]
    all_prices_df = all_prices_df.loc[:,cols]

    print(f'Finished creating final prices dataframe ({len(all_prices_df)} rows)')
    return all_prices_df


################################################################################
# Flask server
################################################################################

from flask import Flask, render_template_string
from flask_compress import Compress
import json

app = Flask(__name__)
Compress(app)

app.config['COMPRESS_MIMETYPES'] = ['text/html', 'text/css', 'text/xml'] # don't compress json
app.config['COMPRESS_LEVEL'] = 6
app.config['COMPRESS_MIN_SIZE'] = 4096

STATE = {}
REFRESH_FREQ_SECONDS = 60 * 20
REFRESHER_RUNNING = False


def generate_handsontable_column_defs(df):
    return [
        {
            'data': col_name, 
            'type': 'numeric' if (df[col_name].dtype == 'int64' or df[col_name].dtype == 'float64') else 'text'
        } for col_name in df.columns
    ]


def build_html(df):
    if df.empty:
        return 'No data'
        
    # convert to json
    data_json = df.to_json(orient='records')
    column_defs_json = json.dumps(generate_handsontable_column_defs(df))
    columns_names_json = json.dumps(list(df.columns))

    # define html template
    html_template = """<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>EC2 Pricing</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/handsontable@11.0.1/dist/handsontable.full.min.css">
        <script src="https://cdn.jsdelivr.net/npm/handsontable@11.0.1/dist/handsontable.full.min.js"></script>
    </head>
    <body>
        <div id="hot"></div>
        <script>
            document.addEventListener('DOMContentLoaded', function() {
                var dataString = '{{ data_json | safe }}';
                var data = JSON.parse(dataString);
                var container = document.getElementById('hot');
                var hot = new Handsontable(container, {
                    licenseKey: 'non-commercial-and-evaluation',
                    data: data,
                    width: '100%',
                    columns: {{ column_defs_json | safe }},
                    columnSorting: true,
                    rowHeaders: true,
                    colHeaders: {{ columns_names_json | safe }},
                    filters: true,
                    dropdownMenu: true,
                    fixedColumnsStart: 6,
                });
            });
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template, data_json=data_json, column_defs_json=column_defs_json, columns_names_json=columns_names_json)


def periodically_update_state():
    while True:
        time.sleep(REFRESH_FREQ_SECONDS / 3)
        update_state(None, ['spot', 'on-demand'])


def update_state(region_code: str, markets: [str]):
    global REFRESHER_RUNNING
    if not REFRESHER_RUNNING:
        print('Starting data refresher ...')
        REFRESHER_RUNNING = True
        refresher = threading.Thread(target=periodically_update_state, args=())
        refresher.start()

    global STATE
    key = 'all' if not region_code else region_code
    print(f'Refreshing data for {key} region ...')
    now = time.time()
    data = get_aws_ec2_prices_df(None if not region_code else [region_code], markets)
    state = {'timestamp_sec': now, 'data': data}
    STATE[key] = state
    if key == 'all':
        for region_code in data['region_code'].unique():
            region_data = data[data['region_code'] == region_code].reset_index(drop=True)
            STATE[region_code] = {'timestamp_sec': now, 'data': region_data, 'html': build_html(region_data), 'json': region_data.to_json(orient='records')}
    state['html'] = build_html(data)
    state['json'] = data.to_json(orient='records')
    return state


def get_and_maybe_update_state(region_code: str, markets: [str], format: str):
    if DEBUG: print(f'Getting and maybe updating state for {region_code} region ...')
    global STATE
    now = time.time()
    key = 'all' if not region_code else region_code
    state = STATE.get(key, None)
    if not state or now - state['timestamp_sec'] > REFRESH_FREQ_SECONDS:
        state = update_state(region_code, markets)
    return state['html'] if format == 'html' else state['json']


@app.route('/')
def get_all_html():
    return get_and_maybe_update_state(None, ['spot', 'on-demand'], 'html')


@app.route('/<region_code>')
def get_region_html(region_code):
    return get_and_maybe_update_state(region_code, ['spot', 'on-demand'], 'html')


@app.route('/api')
def get_all_json():
    return get_and_maybe_update_state(None, ['spot', 'on-demand'], 'json')


@app.route('/api/<region_code>')
def get_region_json(region_code):
    return get_and_maybe_update_state(region_code, ['spot', 'on-demand'], 'json')


def open_browser():
    from werkzeug.serving import is_running_from_reloader
    import webbrowser
    time.sleep(1)
    if not is_running_from_reloader():
        webbrowser.get("chrome").open("http://127.0.0.1:80")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--browser', action='store_true', help='Open browser after starting the server')
    args = parser.parse_args()
    
    if args.browser:
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.start()
        
    app.run(host='0.0.0.0', port=80, debug=True)
