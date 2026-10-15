#!/usr/bin/env python3
"""
AU_CC Report — daily pipeline (Python 3.11, GitHub Actions)

Pulls the AU_CC Google Ads account (EVERY campaign type) and the AppsFlyer lead
event for net.one97.paytm, and writes four raw tabs to the Google Sheet:

  raw           Date, Campaign ID, Campaign, Campaign Type, Impressions, Clicks, Cost (INR), GAds Leads
  af_raw        Date, Campaign ID, Campaign, Campaign Type, AF Leads, AF Click Leads, AF Impression Leads, AF Other Leads
  af_log        one line per AppsFlyer day pulled — the "Final counted" check
  conv_actions  every conversion action in the account, last 30 days

The Apps Script (AU_CC_Scorecard_ALL.gs) only READS these tabs and builds the reports.

Metric definitions
  GAds Leads      the AppsFlyer-imported lead action, as Google Ads counts it
  AF Leads        the same event straight from AppsFlyer raw data (all touch types)
  AF Click Leads  AF Leads whose "Attributed Touch Type" is click
  Click CPL       Cost ÷ AF Click Leads      (built in the Apps Script)
"""
import csv
import io
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import requests
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.oauth2.service_account import Credentials

IST = ZoneInfo('Asia/Kolkata')

# ═════════════════════════════ CONFIG ═════════════════════════════
START_DATE = '2026-06-01'      # first date of Google Ads spend history

# The lead event. Google Ads shows it under its imported-action name;
# AppsFlyer knows it by the bare event name.
LEAD_EVENT_NAME = 'CC_AU_Performance_marketing_bookings_exit'
GADS_LEAD_ACTIONS = [
    'net.one97.paytm (Android) CC_AU_Performance_marketing_bookings_exit 2026-09-21T05:35:43.361',
]
# 'conversions'     = Google Ads "Conversions" column (primary actions only)
# 'all_conversions' = Google Ads "All conv." column (use if the action is secondary)
GADS_CONV_METRIC = 'conversions'

# --- AppsFlyer ---------------------------------------------------
AF_ENABLED = True
AF_START_DATE = '2026-09-21'   # the event was created on this date
AF_LOOKBACK_DAYS = 3           # newest days re-pulled on every run
AF_INCLUDE_RETARGETING = True  # also pull re-engagement (retargeting) events
AF_MEDIA_SOURCE_FILTER = 'googleadwords_int'
AF_EVENT_FILTER_MODE = 'auto'  # 'server', 'local' or 'auto'
AF_SCOPE_MODE = 'campaign_id'  # 'campaign_id' = only AU_CC campaigns; 'off' = keep everything
AF_LAG_DAYS = None             # None = no lag rule; 0 = same-day only; 1 = next day only …
AF_DEDUP_SCOPE = 'daily'       # 'daily' = one lead per user per day; 'off' = every event
AF_DEDUP_FIELDS = ['Customer User ID', 'AppsFlyer ID', 'Advertising ID']
AF_TIMEZONE = 'Asia/Kolkata'   # set to None if AppsFlyer ever rejects it
AF_HOST = 'https://hq1.appsflyer.com'
AF_MAX_ROWS = 1_000_000
AF_REQUEST_TIMEOUT = 180
AF_TIME_BUDGET_SEC = 1500      # whole AppsFlyer stage; the workflow timeout is 45 min
GOOGLE_MEDIA_SOURCES = {'googleadwords_int', 'google', 'google ads', 'google_ads'}
# ══════════════════════════════════════════════════════════════════

RAW_HEADER = ['Date', 'Campaign ID', 'Campaign', 'Campaign Type',
              'Impressions', 'Clicks', 'Cost (INR)', 'GAds Leads']
AF_HEADER = ['Date', 'Campaign ID', 'Campaign', 'Campaign Type',
             'AF Leads', 'AF Click Leads', 'AF Impression Leads', 'AF Other Leads']
AF_LOG_HEADER = ['Date', 'Pulled At (IST)', 'Rows Downloaded', 'Leads Counted',
                 'Click Leads', 'Status']
CONV_HEADER = ['Conversion Action', 'Conversions (last 30d)', 'All Conv. (last 30d)',
               'Counted as GAds Leads']

TYPE_LABELS = {
    'SEARCH': 'Search', 'MULTI_CHANNEL': 'App', 'PERFORMANCE_MAX': 'PMax',
    'DEMAND_GEN': 'Demand Gen', 'DISCOVERY': 'Demand Gen', 'VIDEO': 'Video',
    'DISPLAY': 'Display', 'SHOPPING': 'Shopping', 'SMART': 'Smart',
}


# ───────────────────────────── helpers ─────────────────────────────
def log(msg=''):
    print(msg, flush=True)


def env(name, required=True):
    val = (os.environ.get(name) or '').strip()
    if required and not val:
        sys.exit(f'❌ Missing GitHub secret: {name}')
    return val


def digits(s):
    return re.sub(r'\D', '', s or '')


def norm(s):
    """Letters and digits only, lower case — survives curly quotes, spaces, dashes."""
    return re.sub(r'[^a-z0-9]', '', str(s or '').lower())


GOOGLE_NORM = {norm(m) for m in GOOGLE_MEDIA_SOURCES}


def daterange(start, end):
    d, e = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    out = []
    while d <= e:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def to_num(v):
    try:
        return float(str(v).replace(',', '').strip() or 0)
    except ValueError:
        return 0.0


# ─────────────────────────── Google Ads ───────────────────────────
def build_ga_client():
    return GoogleAdsClient.load_from_dict({
        'developer_token': env('DEVELOPER_TOKEN'),
        'client_id': env('CLIENT_ID'),
        'client_secret': env('CLIENT_SECRET'),
        'refresh_token': env('REFRESH_TOKEN'),
        'login_customer_id': digits(env('LOGIN_CUSTOMER_ID')),
        'use_proto_plus': True,
    })


def run_query(client, cid, query, label):
    svc = client.get_service('GoogleAdsService')
    out = []
    try:
        for batch in svc.search_stream(customer_id=cid, query=query):
            out.extend(batch.results)
    except GoogleAdsException as ex:
        log(f'❌ [{label}] Google Ads query failed (request id {ex.request_id})')
        for err in ex.failure.errors:
            log(f'     {err.message}')
        raise   # stop: better to keep yesterday's sheet than write an empty one
    log(f'   [{label}] {len(out):,} rows')
    return out


def channel_label(client, value):
    name = getattr(value, 'name', None)
    if not name:
        try:
            name = client.enums.AdvertisingChannelTypeEnum(value).name
        except Exception:
            name = str(value)
    return TYPE_LABELS.get(name, name.replace('_', ' ').title())


def fetch_campaign_map(client, cid):
    """campaign id -> current name and type (includes paused/removed campaigns)."""
    q = 'SELECT campaign.id, campaign.name, campaign.advertising_channel_type FROM campaign'
    m = {}
    for r in run_query(client, cid, q, 'campaign list'):
        m[str(r.campaign.id)] = {
            'name': r.campaign.name,
            'type': channel_label(client, r.campaign.advertising_channel_type),
        }
    return m


def fetch_campaign_metrics(client, cid, start, end):
    q = f"""
        SELECT segments.date, campaign.id,
               metrics.impressions, metrics.clicks, metrics.cost_micros
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'"""
    agg = defaultdict(lambda: [0, 0, 0.0])
    for r in run_query(client, cid, q, 'campaign metrics'):
        a = agg[(r.segments.date, str(r.campaign.id))]
        a[0] += r.metrics.impressions
        a[1] += r.metrics.clicks
        a[2] += r.metrics.cost_micros / 1e6
    return agg


def fetch_conversions(client, cid, start, end):
    q = f"""
        SELECT segments.date, campaign.id, segments.conversion_action_name,
               metrics.conversions, metrics.all_conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'"""
    return run_query(client, cid, q, 'conversions by action')


def build_leads(conv_rows, since):
    targets = {norm(a) for a in GADS_LEAD_ACTIONS}
    ev = norm(LEAD_EVENT_NAME)
    leads = defaultdict(float)
    per_action = defaultdict(lambda: [0.0, 0.0])
    for r in conv_rows:
        name = r.segments.conversion_action_name
        conv, allc = r.metrics.conversions, r.metrics.all_conversions
        if r.segments.date >= since:
            per_action[name][0] += conv
            per_action[name][1] += allc
        if norm(name) in targets:
            v = allc if GADS_CONV_METRIC == 'all_conversions' else conv
            if v:
                leads[(r.segments.date, str(r.campaign.id))] += v

    log('')
    log('== ALL CONVERSION ACTIONS IN THIS ACCOUNT (last 30 days) ==')
    for name, (c, a) in sorted(per_action.items(), key=lambda kv: -kv[1][1]):
        mark = '✅' if norm(name) in targets else '  '
        log(f'  {mark} {name}  |  conversions={c:,.1f}  all_conv={a:,.1f}')
    matched = [n for n in per_action if norm(n) in targets]
    if not matched:
        log('⚠️  None of GADS_LEAD_ACTIONS appeared in the last 30 days.')
        near = [n for n in per_action if ev in norm(n)]
        if near:
            log('    Actions containing the event name — copy the right one into GADS_LEAD_ACTIONS:')
            for n in near:
                log(f'      {n}')
    else:
        c = sum(per_action[n][0] for n in matched)
        a = sum(per_action[n][1] for n in matched)
        if c == 0 and a > 0 and GADS_CONV_METRIC == 'conversions':
            log("⚠️  Lead action has All conv. but 0 Conversions — it is probably a SECONDARY "
                "action. Set GADS_CONV_METRIC = 'all_conversions'.")
    log(f'   GAds Leads: {sum(leads.values()):,.1f} across {len(leads):,} date|campaign rows')
    return leads, per_action


def report_channel_breakdown(metrics, id_map, since):
    cost = defaultdict(float)
    for (d, cid), (_, _, c) in metrics.items():
        if d >= since:
            cost[id_map.get(cid, {}).get('type', 'Unknown')] += c
    log('')
    log('== Channel type breakdown (cost, last 30 days) ==')
    for t, c in sorted(cost.items(), key=lambda kv: -kv[1]):
        log(f'   {t:<12} ₹{c:,.0f}')


# ─────────────────────────── Google Sheet ───────────────────────────
def open_sheet():
    info = json.loads(env('GSHEET_SERVICE_ACCOUNT'))
    creds = Credentials.from_service_account_info(
        info, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    log(f'   Sheet is written by: {info.get("client_email")}  (must be an Editor on the Sheet)')
    return gspread.authorize(creds).open_by_key(env('SHEET_ID'))


def find_ws(sh, title):
    """Find a tab ignoring capitals and spaces — Google treats 'Raw' and 'raw' as the same name."""
    want = title.strip().lower()
    for ws in sh.worksheets():
        if ws.title.strip().lower() == want:
            if ws.title != title:
                old = ws.title
                ws.update_title(title)
                log(f'   renamed tab "{old}" → "{title}"')
            return ws
    return None


def get_ws(sh, title, cols):
    return find_ws(sh, title) or sh.add_worksheet(title=title, rows=100, cols=max(cols, 5))


def write_tab(sh, title, header, rows):
    ws = get_ws(sh, title, len(header))
    data = [header] + rows
    ws.clear()
    ws.resize(rows=max(len(data) + 1, 10), cols=len(header))
    step = 10000
    for i in range(0, len(data), step):
        ws.update(range_name=f'A{i + 1}', values=data[i:i + step],
                  value_input_option='RAW')
    log(f'   ✍️  {title}: {len(rows):,} rows')


def read_tab(sh, title):
    ws = find_ws(sh, title)
    if ws is None:
        return []
    vals = ws.get_all_values()
    if len(vals) < 2:
        return []
    head = [h.strip() for h in vals[0]]
    return [dict(zip(head, v)) for v in vals[1:] if any(v)]


# ──────────────────────────── AppsFlyer ────────────────────────────
class AFAuthError(RuntimeError):
    pass


def af_request(app_id, token, day, retarget, event_filter):
    url = f'{AF_HOST}/api/raw-data/export/app/{app_id}/in_app_events_report/v5'
    params = {'from': day, 'to': day, 'maximum_rows': AF_MAX_ROWS}
    if AF_MEDIA_SOURCE_FILTER:
        params['media_source'] = AF_MEDIA_SOURCE_FILTER
    if event_filter:
        params['event_name'] = LEAD_EVENT_NAME
    if AF_TIMEZONE:
        params['timezone'] = AF_TIMEZONE
    if retarget:
        params['reattr'] = 'true'
    resp = requests.get(url, params=params, timeout=AF_REQUEST_TIMEOUT,
                        headers={'Authorization': f'Bearer {token}', 'accept': 'text/csv'})
    if resp.status_code == 401:
        raise AFAuthError('AppsFlyer 401 — API token invalid or revoked. Regenerate the V2 token.')
    if resp.status_code != 200:
        raise RuntimeError(f'AppsFlyer HTTP {resp.status_code}: {resp.text[:300]}')
    rows = list(csv.DictReader(io.StringIO(resp.content.decode('utf-8-sig', errors='replace'))))
    if len(rows) >= AF_MAX_ROWS - 1:
        log(f'⚠️  {day}: hit the {AF_MAX_ROWS:,}-row cap — this day is truncated')
    return rows


def pull_af_day(app_id, token, day, state):
    """Return (lead-event rows, rows downloaded) for one day, UA + retargeting."""
    target = norm(LEAD_EVENT_NAME)
    out, downloaded = [], 0
    for retarget in ([False, True] if AF_INCLUDE_RETARGETING else [False]):
        tag = 'RT' if retarget else 'UA'
        server = state['mode'] in ('server', 'auto')
        rows = af_request(app_id, token, day, retarget, server)
        downloaded += len(rows)
        # 'auto': if the event filter returns nothing, check once without it
        if not rows and state['mode'] == 'auto' and state['fallback'] and not retarget:
            log(f'     {day} {tag}: 0 rows with the event filter — re-pulling unfiltered to check')
            rows = af_request(app_id, token, day, retarget, False)
            downloaded += len(rows)
            if any(norm(x.get('Event Name')) == target for x in rows):
                state['mode'] = 'local'
                log('     event found locally — event_name filter not working; matching locally from now on')
            else:
                state['fallback'] = False
                log('     not in the unfiltered pull either — genuinely no leads that day; fallback off')
        matched = [x for x in rows if norm(x.get('Event Name')) == target]
        for x in matched:
            x['_src'] = tag
        state['src'][tag] += len(matched)
        log(f'     {day} {tag}: downloaded {len(rows):,}, lead rows {len(matched):,}')
        out.extend(matched)
    return out, downloaded


def _real_id(v):
    v = (v or '').strip()
    if not v or v.lower() in ('none', 'null', 'n/a'):
        return ''
    if set(v.replace('-', '')) <= {'0'}:      # zeroed advertising ID (limited ad tracking)
        return ''
    return v


def process_af(rows, id_map, st, scope_on=True):
    """Apply the gates and return {(date, campaign_id): [all, click, impression, other]}."""
    rows.sort(key=lambda x: x.get('Event Time') or '')
    seen_events, seen_users = set(), defaultdict(set)
    out = defaultdict(lambda: [0, 0, 0, 0])
    for x in rows:
        st['in'] += 1
        # 1. media source
        if norm(x.get('Media Source')) not in GOOGLE_NORM:
            st['not_google'] += 1
            continue
        # 2. account scope — the app is shared with other teams' Google Ads accounts
        cid = (x.get('Campaign ID') or '').strip()
        if scope_on and cid not in id_map:
            st['other_account'] += 1
            st['foreign'][x.get('Campaign') or cid or '(blank)'] += 1
            continue
        # 3. lag
        ev_date = (x.get('Event Time') or '')[:10]
        try:
            lag = (date.fromisoformat(ev_date)
                   - date.fromisoformat((x.get('Attributed Touch Time') or '')[:10])).days
            st['lags'][str(lag) if lag < 8 else '8+'] += 1
        except ValueError:
            lag = None
            st['lags']['?'] += 1
        if AF_LAG_DAYS is not None and lag != AF_LAG_DAYS:
            st['lag'] += 1
            continue
        # 4. exact duplicate event (same event can appear in UA and RT reports)
        ekey = (x.get('Event Time'), x.get('AppsFlyer ID'), x.get('Event Name'))
        if ekey in seen_events:
            st['dup_event'] += 1
            continue
        seen_events.add(ekey)
        # 5. one lead per user per day
        if AF_DEDUP_SCOPE == 'daily':
            ids = [f'{f}:{_real_id(x.get(f))}' for f in AF_DEDUP_FIELDS if _real_id(x.get(f))]
            if any(i in seen_users[ev_date] for i in ids):
                st['dup_user'] += 1
                continue
            seen_users[ev_date].update(ids)
        # count, split by attributed touch type
        tt = (x.get('Attributed Touch Type') or '').strip().lower()
        a = out[(ev_date, cid)]
        a[0] += 1
        if tt == 'click':
            a[1] += 1
        elif tt in ('impression', 'view'):
            a[2] += 1
        else:
            a[3] += 1
        st['touch'][tt or '(blank)'] += 1
        st['counted'] += 1
    return out


def run_appsflyer(sh, id_map, yesterday):
    app_id = env('APPSFLYER_APP_ID', False)
    token = env('APPSFLYER_API_TOKEN', False)
    if not (AF_ENABLED and app_id and token):
        log('⏭️  AppsFlyer skipped (disabled or secrets missing) — af_raw left as it was')
        return

    log('')
    log('== AppsFlyer ==')
    log(f'   app {app_id} · event {LEAD_EVENT_NAME} · scope {AF_SCOPE_MODE} · '
        f'lag {AF_LAG_DAYS} · dedup {AF_DEDUP_SCOPE} · retargeting {AF_INCLUDE_RETARGETING}')
    scope_on = AF_SCOPE_MODE == 'campaign_id'
    if scope_on and not id_map:
        log('⚠️  campaign list is empty — scope would drop everything; switching scope off')
        scope_on = False

    existing = read_tab(sh, 'af_raw')
    old_log = {r.get('Date'): r for r in read_tab(sh, 'af_log')}
    done_before = {d for d, r in old_log.items() if r.get('Status') == 'ok'}

    all_days = daterange(AF_START_DATE, yesterday)
    lb_start = (yesterday - timedelta(days=AF_LOOKBACK_DAYS - 1)).isoformat()
    recent = [d for d in all_days if d >= lb_start][::-1]
    backfill = [d for d in all_days if d < lb_start and d not in done_before][::-1]
    todo = recent + backfill
    log(f'   days to pull: {len(recent)} recent + {len(backfill)} backfill')

    st = {'in': 0, 'not_google': 0, 'other_account': 0, 'lag': 0, 'dup_event': 0,
          'dup_user': 0, 'counted': 0, 'foreign': Counter(), 'lags': Counter(),
          'touch': Counter()}
    state = {'mode': AF_EVENT_FILTER_MODE, 'fallback': True, 'src': Counter()}
    new_rows, new_log = {}, {}
    now = datetime.now(IST).strftime('%Y-%m-%d %H:%M')
    t0 = time.time()

    for d in todo:
        if time.time() - t0 > AF_TIME_BUDGET_SEC:
            log('⏱️  time budget reached — remaining days will be pulled on the next run')
            break
        try:
            rows, downloaded = pull_af_day(app_id, token, d, state)
        except AFAuthError as e:
            log(f'❌ {e}')
            break
        except Exception as e:  # keep yesterday's rows for this day
            log(f'❌ {d}: {e}')
            new_log[d] = [d, now, 0, 0, 0, f'error: {str(e)[:80]}']
            continue
        before_counted = st['counted']
        agg = process_af(rows, id_map, st, scope_on)
        new_rows[d] = [
            [k[0], k[1], id_map.get(k[1], {}).get('name', ''),
             id_map.get(k[1], {}).get('type', 'Other'), *v]
            for k, v in agg.items()]
        clicks = sum(v[1] for v in agg.values())
        new_log[d] = [d, now, downloaded, st['counted'] - before_counted, clicks, 'ok']

    # merge: keep old rows for days not re-pulled this run
    final = []
    for r in existing:
        d = r.get('Date', '')
        if d in new_rows or d < AF_START_DATE:
            continue
        cid = r.get('Campaign ID', '')
        info = id_map.get(cid, {})
        final.append([d, cid, info.get('name') or r.get('Campaign', ''),
                      info.get('type') or r.get('Campaign Type', 'Other'),
                      int(to_num(r.get('AF Leads'))), int(to_num(r.get('AF Click Leads'))),
                      int(to_num(r.get('AF Impression Leads'))),
                      int(to_num(r.get('AF Other Leads')))])
    for rows in new_rows.values():
        final.extend(rows)
    final.sort(key=lambda r: (r[0], r[1]))

    log_rows = [[r.get(h, '') for h in AF_LOG_HEADER] for d, r in old_log.items()
                if d not in new_log]
    log_rows += list(new_log.values())
    log_rows.sort(key=lambda r: r[0], reverse=True)

    log('')
    log('== AppsFlyer summary (this run) ==')
    log(f'   lead rows downloaded: {st["in"]:,}  (UA {state["src"]["UA"]:,} · RT {state["src"]["RT"]:,})')
    log(f'   dropped – not Google Ads:          {st["not_google"]:,}')
    log(f'   dropped – other accounts\' campaigns: {st["other_account"]:,}')
    for name, n in st['foreign'].most_common(5):
        log(f'        e.g. {name}: {n:,}')
    log(f'   dropped – lag rule:                {st["lag"]:,}')
    log(f'   dropped – duplicate event:         {st["dup_event"]:,}')
    log(f'   dropped – same user same day:      {st["dup_user"]:,}')
    log(f'   Final counted: {st["counted"]:,}   touch types: {dict(st["touch"])}')
    log(f'   lag (days touch→event): {dict(sorted(st["lags"].items()))}')

    write_tab(sh, 'af_raw', AF_HEADER, final)
    write_tab(sh, 'af_log', AF_LOG_HEADER, log_rows)
    return final


# ─────────────────────────────── main ───────────────────────────────
def main():
    yesterday = (datetime.now(IST) - timedelta(days=1)).date()
    y = yesterday.isoformat()
    since30 = (yesterday - timedelta(days=29)).isoformat()
    log(f'AU_CC Report · Google Ads {START_DATE} → {y} · AppsFlyer from {AF_START_DATE}')

    client = build_ga_client()
    cid = digits(env('CLIENT_CUSTOMER_ID'))
    log(f'   Google Ads account: {cid}')
    id_map = fetch_campaign_map(client, cid)
    metrics = fetch_campaign_metrics(client, cid, START_DATE, y)
    conv_rows = fetch_conversions(client, cid, START_DATE, y)
    leads, per_action = build_leads(conv_rows, since30)
    report_channel_breakdown(metrics, id_map, since30)

    raw_rows = []
    for key in sorted(set(metrics) | set(leads)):
        d, c = key
        impr, clicks, cost = metrics.get(key, [0, 0, 0.0])
        info = id_map.get(c, {})
        raw_rows.append([d, c, info.get('name', ''), info.get('type', 'Other'),
                         impr, clicks, round(cost, 2), round(leads.get(key, 0.0), 2)])

    targets = {norm(a) for a in GADS_LEAD_ACTIONS}
    conv_tab = [[n, round(c, 2), round(a, 2), 'YES' if norm(n) in targets else '']
                for n, (c, a) in sorted(per_action.items(), key=lambda kv: -kv[1][1])]

    log('')
    log('== Writing sheet ==')
    sh = open_sheet()
    write_tab(sh, 'raw', RAW_HEADER, raw_rows)
    write_tab(sh, 'conv_actions', CONV_HEADER, conv_tab)

    af_rows = run_appsflyer(sh, id_map, yesterday)

    # month-to-date check, so the log alone answers "did it work?"
    ms = y[:8] + '01'
    cost = sum(r[6] for r in raw_rows if r[0] >= ms)
    gl = sum(r[7] for r in raw_rows if r[0] >= ms)
    log('')
    log(f'== MTD check ({ms} → {y}) ==')
    log(f'   Cost ₹{cost:,.0f} · GAds Leads {gl:,.1f} · GAds CPL '
        f'{"₹%.2f" % (cost / gl) if gl else "—"}')
    if af_rows is not None:
        al = sum(r[4] for r in af_rows if r[0] >= ms)
        ck = sum(r[5] for r in af_rows if r[0] >= ms)
        log(f'   AF Leads {al:,} · AF Click Leads {ck:,} · Click CPL '
            f'{"₹%.2f" % (cost / ck) if ck else "—"}')
    log('✅ Done')


if __name__ == '__main__':
    main()
