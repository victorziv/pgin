import sys
import os
import datetime
import time
from psycopg2.extensions import AsIs
import re
SPACE_PATTERN = re.compile('\s+')  # noqa
# ==================================


def calculate_duedate(created, rundays, include_weekends):
    days = []
    d = created
    while len(days) < rundays:

        if include_weekends:
            days.append(d.date())
        else:
            if not d.weekday() in [4, 5]:
                days.append(d.date())

        d += datetime.timedelta(days=1)
    return days[-1]
# ____________________________


def compare_versions(ver1, ver2):
    version1 = ver1.split('-', 2)[0].strip()
    version2 = ver2.split('-', 2)[0].strip()
    v = [tuple([int(x) for x in n.split('.')]) for n in [version1, version2]]
    return ((v[0] > v[1]) - (v[0] < v[1]))
# _________________________________


def compose_query_order(sort_field, sort_order):
    sf = [f.strip() for f in sort_field.split(',')]
    so = [o.strip() for o in sort_order.split(',')]
    sort_zip = list(zip(sf, so))

    query = """
        ORDER BY
    """
    order_placeholders = ','.join(['%s %s' for i in range(len(sort_zip))])
    query += order_placeholders
    order_params = [AsIs(p) for t in sort_zip for p in t]
    return query, order_params
# ____________________________


def convert_to_utc(local_time):

    # NOTE. This is hard-coded for time-zones east to GMT

    local_str, offset_str = local_time.split('+')
    off_h, off_m = int(offset_str[:2]), int(offset_str[2:])

    offset = datetime.timedelta(hours=off_h, minutes=off_m)
    naive = datetime.datetime.strptime(local_str, "%Y-%m-%dT%H:%M:%S.%f")
    u = naive - offset
    return datetime.datetime.strftime(u, "%Y-%m-%d %H:%M")
# ____________________________


def create_directory(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno == 17:
                pass
# ____________________________


def display_cmd(logger, cmd):

    if not len(cmd):
        return

    logger.info("CMD:")
    logger.info('%s' % cmd)
# _________________________________


def display_cmd_output(logger, output):

    if not len(output):
        return

    logger.info("CMD Output:")
    logger.info('%s' % '\n'.join(output))
# _________________________________


def datetime_ob_to_str(do):

    if isinstance(do, datetime.datetime):
        return datetime.datetime.strftime(do, '%Y-%m-%d %H:%M:%S')

    return do
# ____________________________


def datetime_str_to_ob(ds):

    if type(ds).__name__ == 'str':
        return datetime.datetime.strptime(ds, '%Y-%m-%d %H:%M:%S')

    return ds
# ____________________________


def generate_uid():

    return "%s.%s" %\
        (
            datetime.datetime.fromtimestamp(time.time()).strftime("%Y%m%d%H%M%S"),
            datetime.datetime.now().microsecond,
        )
# ____________________________


def get_callee_name(depth=0):
    return sys._getframe(depth + 1).f_code.co_name
# ____________________________


def get_inventory_token():
    token_file = os.path.expanduser("~/.infinidat/.inventory-api-token")

    with open(token_file, "r") as f:
        token = f.read()

    return token
# ____________________________


def replace_spaces_with_undersore(s):
    return SPACE_PATTERN.sub('_', s)
# ____________________________


def timestamps_to_string(tsk):
    for k in tsk:
        tsk[k] = datetime_ob_to_str(tsk[k])
    return tsk
