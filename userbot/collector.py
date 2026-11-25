"""GiftHawk collector v5 — روی GitHub Actions به‌صورت «پیوسته» (حدود ۵۴ دقیقه در هر اجرا) کار می‌کند.

⚠️ تست‌نشده با تلگرام واقعی. فرض‌ها (اگر خطا داد، متن لاگ را ببین و همین فایل را اصلاح کن):
  • نسخهٔ telethon باید متدهای GetStarGiftsRequest و GetResaleStarGiftsRequest را داشته باشد.
  • نام فیلدهای StarGift/StarGiftUnique (resell_amount، rarity یا rarity_permille، attributes، availability_resale)
    بین لایه‌ها فرق کرده؛ کد عمداً با getattr دفاعی نوشته شده و آنچه نفهمد را رد می‌کند.
  • مینی‌اپ Portals (short name = market) و Tonnel برای گرفتن initData حدسی‌اند و با env قابل تغییرند.

کارها (حساب userbot باید در بات «ادمین» باشد):
  ۱) هر ~۳۰ دقیقه: فهرست «کامل» کالکشن‌های گیفت تلگرام → /cols   (دیگر هیچ کالکشنی دستی اضافه نمی‌شود)
  ۲) پیوسته (هر چرخه ~۴۵–۶۰ ثانیه): ارزان‌ترین آگهی‌های ریسِیل هر کالکشنِ دارای آگهی → /ingest
     فقط اگر چیزی تغییر کرده باشد (یا هر ~۸ دقیقه برای تازه‌ماندن)؛ بات همان لحظه ارزیابی و ارسال می‌کند.
  ۳) آهسته و چرخشی: مدل/پس‌زمینه/نمادهای هر کالکشن → /cat (برای دکمه‌های انتخاب در بات)
  ۴) هر ۵ دقیقه نرخ‌ها (/rate) و هر ~۳۰ دقیقه initData تازهٔ Portals/Tonnel (/setsecret)
"""
import asyncio, json, os, statistics, time
import urllib.request
from urllib.parse import urlparse, parse_qs

from telethon import TelegramClient, functions, types, errors
from telethon.sessions import StringSession

API_ID = int(os.environ['TG_API_ID'])
API_HASH = os.environ['TG_API_HASH']
SESSION = os.environ['TG_SESSION']
BOT = os.environ['BOT_USERNAME'].lstrip('@')
RUN_MINUTES = float(os.environ.get('RUN_MINUTES', '54'))          # مدت زندگی هر اجرا؛ اجرای بعدی با cron + صف concurrency ادامه می‌دهد
PER_COL = int(os.environ.get('PER_COL', '40'))                    # چند آگهی ارزان از هر کالکشن
RPS = float(os.environ.get('RPS', '2.5'))                         # سقف درخواست به تلگرام در ثانیه (محتاطانه؛ FloodWait خودکار مدیریت می‌شود)
MIN_CYCLE_S = float(os.environ.get('MIN_CYCLE_S', '45'))
RESEND_S = int(os.environ.get('RESEND_S', '480'))                 # حتی بدون تغییر، هر چند ثانیه یک‌بار دوباره بفرست (بات داده‌ی کهنه‌تر از ۴۰ دقیقه را نادیده می‌گیرد)
CAT_PER_CYCLE = int(os.environ.get('CAT_PER_CYCLE', '3'))
CAT_EVERY_S = int(os.environ.get('CAT_EVERY_S', str(6 * 3600)))
FORCE_TOKENS = os.environ.get('REFRESH_TOKENS', '') == '1'
PORTALS_BOT, PORTALS_APP = os.environ.get('PORTALS_BOT', 'portals'), os.environ.get('PORTALS_APP', 'market')
TONNEL_BOT, TONNEL_URL = os.environ.get('TONNEL_BOT', 'tonnel_network_bot'), os.environ.get('TONNEL_URL', 'https://market.tonnel.network/')
MSG_LIMIT = 3800                                                 # حد پیام تلگرام ۴۰۹۶ کاراکتر است

t0 = time.time()
log = lambda *a: print(f'[collector {time.time() - t0:6.0f}s]', *a, flush=True)
left = lambda: RUN_MINUTES * 60 - (time.time() - t0)


class Pacer:
    """فاصلهٔ ثابت بین درخواست‌ها + مدیریت FloodWait."""
    def __init__(self, rps): self.gap, self.last = 1.0 / rps, 0.0

    async def call(self, c, req):
        for _ in range(3):
            wait = self.gap - (time.time() - self.last)
            if wait > 0: await asyncio.sleep(wait)
            self.last = time.time()
            try: return await c(req)
            except errors.FloodWaitError as e:
                log('FloodWait', e.seconds)
                if e.seconds > 120: raise
                await asyncio.sleep(e.seconds + 1)
        raise RuntimeError('FloodWait مکرر')


pacer = Pacer(RPS)
_last_send = [0.0]


async def to_bot(c, text):
    """پیام به بات؛ parse_mode=None تا مارک‌داون تلگرام (_ و * و `) JSON و توکن‌ها را خراب نکند."""
    for _ in range(3):
        wait = 0.4 - (time.time() - _last_send[0])
        if wait > 0: await asyncio.sleep(wait)
        _last_send[0] = time.time()
        try: await c.send_message(BOT, text, parse_mode=None, link_preview=False); return
        except errors.FloodWaitError as e:
            log('FloodWait(send)', e.seconds)
            if e.seconds > 120: raise
            await asyncio.sleep(e.seconds + 1)
    raise RuntimeError('FloodWait مکرر در ارسال')


# ───────────────────────── ابزار استخراج از آبجکت‌های تلگرام ─────────────────────────
def stars_price(g):
    for a in (getattr(g, 'resell_amount', None) or []):
        if a.__class__.__name__ == 'StarsAmount': return int(a.amount)
    v = getattr(g, 'resell_stars', None)
    return int(v) if v else None


def permille(a):
    """کمیابی به هزارم: هم لایهٔ قدیمی (rarity_permille) هم جدید (rarity.permille)؛ ردهٔ نامی بدون عدد → -1."""
    v = getattr(a, 'rarity_permille', None)
    if isinstance(v, int): return v
    p = getattr(getattr(a, 'rarity', None), 'permille', None)
    return p if isinstance(p, int) else -1


def attr_kind(a):
    n = a.__class__.__name__
    return 'm' if n.endswith('Model') else 'b' if n.endswith('Backdrop') else 's' if n.endswith('Pattern') else None


def attrs(g):
    out = {'m': ('', -1), 'b': ('', -1), 's': ('', -1)}
    for a in (getattr(g, 'attributes', None) or []):
        k = attr_kind(a)
        if k: out[k] = (getattr(a, 'name', '') or '', permille(a))
    return out


def row(g, listed_at):
    p, num = stars_price(g), getattr(g, 'num', None)
    if not p or not isinstance(num, int): return None
    a = attrs(g)
    return [num, p, a['m'][0], a['m'][1], a['b'][0], a['b'][1], a['s'][0], a['s'][1], listed_at]


# ───────────────────────── کاتالوگ کالکشن‌ها ─────────────────────────
async def all_gifts(c):
    r = await pacer.call(c, functions.payments.GetStarGiftsRequest(hash=0))
    return [g for g in (getattr(r, 'gifts', None) or []) if getattr(g, 'title', None)]


def pack(items, make):
    """آیتم‌ها را در چند پیام ≤ MSG_LIMIT بسته‌بندی می‌کند؛ make(لیست، شمارهٔ بخش، آخرین؟) → متن."""
    parts, cur = [], []
    for it in items:
        if cur and len(make(cur + [it], 99, False)) > MSG_LIMIT:
            parts.append(cur); cur = []
        cur.append(it)
    if cur or not parts: parts.append(cur)
    return [make(p, i, i == len(parts) - 1) for i, p in enumerate(parts)]


async def send_cols(c, gifts):
    items = [[g.title, int(getattr(g, 'availability_resale', 0) or 0)] for g in gifts]
    for msg in pack(items, lambda l, i, last: '/cols ' + json.dumps({'l': l}, ensure_ascii=False, separators=(',', ':'))):
        await to_bot(c, msg)
    log(f'کاتالوگ: {len(items)} کالکشن به بات فرستاده شد')


async def resale(c, gift_id, **kw):
    req = dict(gift_id=gift_id, offset='', **kw)
    try: return await pacer.call(c, functions.payments.GetResaleStarGiftsRequest(stars_only=True, **req))
    except TypeError: return await pacer.call(c, functions.payments.GetResaleStarGiftsRequest(**req))   # لایهٔ قدیمی‌تر stars_only ندارد


async def send_catalog(c, g):
    """مدل/پس‌زمینه/نمادهای یک کالکشن (برای دکمه‌های انتخاب در بات)."""
    res = await resale(c, g.id, limit=1, attributes_hash=0)
    by = {'m': [], 'b': [], 's': []}
    for a in (getattr(res, 'attributes', None) or []):
        k = attr_kind(a)
        name = getattr(a, 'name', None)
        if k and name: by[k].append([name, permille(a)])
    n = 0
    for k, items in by.items():
        if not items: continue
        for msg in pack(items, lambda l, i, last, k=k: '/cat ' + json.dumps({'c': g.title, 'k': k, 'p': i, 'l': l}, ensure_ascii=False, separators=(',', ':'))):
            await to_bot(c, msg)
        n += len(items)
    return n


# ───────────────────────── آگهی‌ها ─────────────────────────
class Snap:
    def __init__(self): self.sig, self.sent_at, self.first = None, 0.0, {}


snaps: dict = {}


def ingest_msgs(col, rows):
    def make(l, i, last):
        d = {'c': col, 'l': l}
        if i > 0: d['a'] = 1
        if not last: d['m'] = 1
        return '/ingest ' + json.dumps(d, ensure_ascii=False, separators=(',', ':'))
    return pack(rows, make)


async def poll_collection(c, g):
    res = await resale(c, g.id, limit=PER_COL, sort_by_price=True)
    s = snaps.setdefault(g.title, Snap())
    now_ms, rows = int(time.time() * 1000), []
    first_poll = s.sig is None
    for u in (getattr(res, 'gifts', None) or []):
        p, num = stars_price(u), getattr(u, 'num', None)
        if not p or not isinstance(num, int): continue
        prev = s.first.get(num)
        if prev is None or p < prev[0]:                           # آگهی تازه یا کاهش قیمت = «تازه آگهی شد»
            s.first[num] = (p, 0 if first_poll else now_ms)
        r = row(u, s.first[num][1])
        if r: rows.append(r)
    sig = tuple(sorted((r[0], r[1]) for r in rows))
    changed = sig != s.sig
    if not changed and time.time() - s.sent_at < RESEND_S: return 0
    for msg in ingest_msgs(g.title, rows): await to_bot(c, msg)
    s.sig, s.sent_at = sig, time.time()
    return 1 if changed else 0


# ───────────────────────── نرخ‌ها و توکن‌ها (مثل نسخهٔ قبل) ─────────────────────────
def init_data(url: str) -> str:
    frag = parse_qs(urlparse(url).fragment)
    return (frag.get('tgWebAppData') or [''])[0]


async def refresh_tokens(c):
    try:
        bot = await c.get_input_entity(PORTALS_BOT)
        r = await c(functions.messages.RequestAppWebViewRequest(peer=bot, app=types.InputBotAppShortName(bot_id=bot, short_name=PORTALS_APP), platform='android', write_allowed=True))
        d = init_data(r.url)
        if d: await to_bot(c, f'/setsecret portals_auth tma {d}'); log('portals token ok')
        else: log('portals: initData پیدا نشد')
    except Exception as e: log('portals token FAIL:', repr(e)[:200])
    try:
        bot = await c.get_input_entity(TONNEL_BOT)
        r = await c(functions.messages.RequestWebViewRequest(peer=bot, bot=bot, platform='android', url=TONNEL_URL))
        d = init_data(r.url)
        if d: await to_bot(c, f'/setsecret tonnel_auth {d}'); log('tonnel token ok')
        else: log('tonnel: initData پیدا نشد')
    except Exception as e: log('tonnel token FAIL:', repr(e)[:200])


TON_SOURCES = [
    ('okx', 'https://www.okx.com/api/v5/market/ticker?instId=TON-USDT', lambda j: j['data'][0]['last']),
    ('kucoin', 'https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=TON-USDT', lambda j: j['data']['price']),
    ('gate', 'https://api.gateio.ws/api/v4/spot/tickers?currency_pair=TON_USDT', lambda j: j[0]['last']),
    ('bybit', 'https://api.bybit.com/v5/market/tickers?category=spot&symbol=TONUSDT', lambda j: j['result']['list'][0]['lastPrice']),
    ('binance', 'https://api.binance.com/api/v3/ticker/price?symbol=TONUSDT', lambda j: j['price']),
    ('coingecko', 'https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd', lambda j: j['the-open-network']['usd']),
]


def http_json(url):
    req = urllib.request.Request(url, headers={'accept': 'application/json', 'user-agent': 'gifthawk/1'})
    with urllib.request.urlopen(req, timeout=8) as r: return json.loads(r.read())


def cfg_num(cfg, key):
    for kv in (getattr(cfg, 'value', None) or []):
        if getattr(kv, 'key', None) == key:
            try: return float(getattr(kv.value, 'value', None))
            except (TypeError, ValueError): return None
    return None


def per_star(x1000):
    if not x1000 or x1000 <= 0: return None
    v = x1000 / 100000 if x1000 >= 100 else x1000 / 1000
    return v if 0.004 <= v <= 0.05 else None


async def push_rates(c):
    prices = []
    for name, url, ex in TON_SOURCES:
        try:
            v = float(ex(await asyncio.to_thread(http_json, url)))
            if 0.05 < v < 1000: prices.append(v)
        except Exception as e: log('ton source', name, 'FAIL', repr(e)[:80])
    sell = wd = None
    try:
        r = await pacer.call(c, functions.help.GetAppConfigRequest(hash=0))
        cfg = getattr(r, 'config', None)
        sell, wd = per_star(cfg_num(cfg, 'stars_usd_sell_rate_x1000')), per_star(cfg_num(cfg, 'stars_usd_withdraw_rate_x1000'))
        tv = cfg_num(cfg, 'ton_usd_rate')
        if tv and 0.05 < tv < 1000: prices.append(tv)
    except Exception as e: log('app config FAIL', repr(e)[:160])
    out = {}
    if prices:
        m = statistics.median(prices)
        kept = [p for p in prices if abs(p - m) / m <= 0.05]
        out['t'], out['n'] = round(statistics.median(kept), 4), len(kept)
    if sell: out['s'] = round(sell, 6)
    if wd: out['w'] = round(wd, 6)
    if not out: log('rates: هیچ منبعی جواب نداد'); return
    await to_bot(c, '/rate ' + json.dumps(out, separators=(',', ':')))
    log('rates sent', out)


# ───────────────────────── حلقهٔ اصلی ─────────────────────────
async def main():
    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as c:
        last = {'rates': 0.0, 'tokens': 0.0 if not FORCE_TOKENS else -1e9, 'cols': 0.0}
        gifts, cat_at, cat_i, cycle = [], {}, 0, 0
        due = lambda k, every: time.time() - last[k] >= every
        while left() > 20:
            cs = time.time(); cycle += 1
            try:
                if due('rates', 300): await push_rates(c); last['rates'] = time.time()
                if due('tokens', 1800): await refresh_tokens(c); last['tokens'] = time.time()
                if due('cols', 1800) or not gifts:
                    gifts = await all_gifts(c); await send_cols(c, gifts); last['cols'] = time.time()
            except Exception as e: log('setup FAIL:', repr(e)[:200])
            # کالکشن‌های دارای آگهی هر چرخه؛ بقیه (فعلاً بی‌آگهی) هر سومین چرخه تا اولین آگهی‌شان هم دیر نماند
            live = [g for g in gifts if (getattr(g, 'availability_resale', 0) or 0) > 0 or cycle % 3 == 0]
            changed = 0
            for g in live:
                if left() < 20: break
                try: changed += await poll_collection(c, g)
                except Exception as e: log('poll FAIL', g.title, repr(e)[:160])
            # کاتالوگ ویژگی‌ها: چند کالکشن در هر چرخه، هر کالکشن هر ~۶ ساعت
            todo = [g for g in gifts if time.time() - cat_at.get(g.title, 0) > CAT_EVERY_S][:CAT_PER_CYCLE]
            for g in todo:
                if left() < 20: break
                try: n = await send_catalog(c, g); cat_at[g.title] = time.time(); log(f'cat {g.title}: {n} ویژگی')
                except Exception as e: log('cat FAIL', g.title, repr(e)[:160]); cat_at[g.title] = time.time() - CAT_EVERY_S + 900   # ۱۵ دقیقه بعد دوباره
            log(f'چرخهٔ {cycle}: {len(live)} کالکشن، {changed} تغییر، {time.time() - cs:.0f}s')
            await asyncio.sleep(max(0, MIN_CYCLE_S - (time.time() - cs)))


asyncio.run(main())
