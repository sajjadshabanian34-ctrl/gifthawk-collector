"""GiftHawk collector — نسخهٔ نهایی (v5.2). روی GitHub Actions پیوسته (~۵۴ دقیقه در هر اجرا) کار می‌کند.

کارها (حساب userbot باید در بات «ادمین» باشد):
  ۱) فهرست کالکشن‌های گیفت تلگرام → /cols      ۲) ارزان‌ترین آگهی‌های ریسِیل هر کالکشن → /ingest (فقط وقت تغییر)
  ۳) مدل/پس‌زمینه/نمادها → /cat                ۴) نرخ‌ها → /rate  و توکن Portals/Tonnel → /setsecret

سه لایهٔ پیدا کردن نام کالکشن‌ها (اگر یکی کار نکرد بعدی):
  الف) payments.getStarGifts و فیلد title    ب) نام از اولین آگهی ریسِیل همان گیفت    ج) فهرست داخلی + payments.getUniqueStarGift
هر جا چیزی نخواند، دلیلش (نام کلاس‌ها و فیلدها) در لاگ چاپ می‌شود؛ هیچ‌چیز محرمانه‌ای چاپ نمی‌شود.
"""
import asyncio, json, os, statistics, sys, time
import urllib.request
from urllib.parse import urlparse, parse_qs

from telethon import TelegramClient, functions, types, errors
from telethon.sessions import StringSession

E = os.environ.get
API_ID, API_HASH, SESSION, BOT = int(E('TG_API_ID') or 0), E('TG_API_HASH', ''), E('TG_SESSION', ''), (E('BOT_USERNAME') or '').lstrip('@')
RUN_MINUTES = float(E('RUN_MINUTES', '54'))
PER_COL = int(E('PER_COL', '40'))
RPS = float(E('RPS', '2.5'))
MIN_CYCLE_S = float(E('MIN_CYCLE_S', '45'))
RESEND_S = int(E('RESEND_S', '480'))
CAT_PER_CYCLE = int(E('CAT_PER_CYCLE', '3'))
CAT_EVERY_S = int(E('CAT_EVERY_S', str(6 * 3600)))
PORTALS_BOT, PORTALS_APP = E('PORTALS_BOT', 'portals'), E('PORTALS_APP', 'market')
TONNEL_BOT, TONNEL_URL = E('TONNEL_BOT', 'tonnel_network_bot'), E('TONNEL_URL', 'https://market.tonnel.network/')
MSG_LIMIT = 3800
SEED = ['Plush Pepe', 'Heart Locket', "Durov's Cap", 'Precious Peach', 'Heroic Helmet', 'Nail Bracelet', 'Astral Shard', 'Mighty Arm', 'Perfume Bottle', 'Bonded Ring', 'Scared Cat', 'Loot Bag', 'Vintage Cigar', 'Genie Lamp', 'Swiss Watch', 'Neko Helmet', 'Eternal Rose', 'Electric Skull', 'Diamond Ring', 'Mini Oscar', 'Gem Signet', 'Toy Bear', 'Santa Hat', 'Signet Ring', 'Top Hat', 'Love Potion', 'Magic Potion', 'Kissed Frog', 'Voodoo Doll', 'Trapped Heart', 'Sharp Tongue', 'Skull Flower', 'Cookie Heart', 'Jester Hat', 'Witch Hat', 'Hex Pot', 'Evil Eye', 'Homemade Cake', 'Sleigh Bell', 'Ginger Cookie', 'Snow Globe', 'Spiced Wine', 'Jingle Bells', 'Snow Mittens', 'Winter Wreath', 'Light Sword', 'Berry Box', 'Bunny Muffin', 'Desk Calendar', 'B-Day Candle', 'Lol Pop', 'Jelly Bunny', 'Hypno Lollipop', 'Candy Cane', 'Big Year', 'Party Sparkler', 'Pet Snake', 'Snake Box', 'Lunar Snake', 'Record Player', 'Spy Agaric', 'Star Notepad', 'Easter Egg', 'Tama Gadget', 'Ion Gem', 'Swag Bag', 'Flying Broom', 'Crystal Ball', 'Bow Tie', 'Restless Jar', 'Joyful Bundle', 'Whip Cupcake', 'Instant Ramen', 'Faith Amulet', 'Mad Pumpkin', 'Input Key', 'Cupid Charm', 'Valentine Box', 'Holiday Drink', 'Xmas Stocking', 'Pool Float', 'Moon Pendant', 'Lush Bouquet', 'Sakura Flower', 'Ice Cream', 'Westside Sign', 'Low Rider', 'Snoop Dogg', 'Snoop Cigar', "Khabib's Papakha", 'UFC Strike', 'Victory Medal', 'Chill Flame', 'Stellar Rocket', 'Siren Chime', 'Jack-in-the-Box', 'Ionic Dryer', 'Gift Box']

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
    """پیام به بات؛ parse_mode=None تا مارک‌داون JSON و توکن‌ها را خراب نکند."""
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


_warned = set()
def warn_once(key, *msg):
    if key not in _warned: _warned.add(key); log('⚠️', *msg)


def keys_of(o):
    try: return ','.join(sorted(o.to_dict().keys()))[:400]
    except Exception: return ','.join(a for a in dir(o) if not a.startswith('_'))[:400]


# ───────────────────────── استخراج از آبجکت‌های تلگرام (دفاعی، چند نام فیلد) ─────────────────────────
def stars_price(g):
    ra = getattr(g, 'resell_amount', None)
    for a in (ra if isinstance(ra, (list, tuple)) else [ra]):
        if a is not None and a.__class__.__name__ == 'StarsAmount':
            try: return int(a.amount)
            except Exception: pass
    for k in ('resell_stars', 'resale_stars'):
        v = getattr(g, k, None)
        if isinstance(v, int) and v > 0: return v
    return None


def permille(a):
    """کمیابی به هزارم: rarity_permille (قدیم)، rarity.permille (جدید)؛ ردهٔ نامی بدون عدد → -1."""
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


def pack(items, make):
    """آیتم‌ها را در چند پیام ≤ MSG_LIMIT بسته‌بندی می‌کند؛ make(لیست، شمارهٔ بخش، آخرین؟) → متن."""
    parts, cur = [], []
    for it in items:
        if cur and len(make(cur + [it], 99, False)) > MSG_LIMIT:
            parts.append(cur); cur = []
        cur.append(it)
    if cur or not parts: parts.append(cur)
    return [make(p, i, i == len(parts) - 1) for i, p in enumerate(parts)]


# ───────────────────────── فراخوانی‌های تلگرام ─────────────────────────
async def resale(c, gift_id, **kw):
    req = dict(gift_id=gift_id, offset='', **kw)
    try: r = functions.payments.GetResaleStarGiftsRequest(stars_only=True, **req)
    except TypeError: r = functions.payments.GetResaleStarGiftsRequest(**req)      # لایهٔ قدیمی‌تر stars_only ندارد
    return await pacer.call(c, r)


_names: dict = {}      # gift_id → نام کالکشن
_diag_done = set()


async def name_from_resale(c, gid):
    res = await resale(c, gid, limit=1)
    for u in (getattr(res, 'gifts', None) or []):
        n = getattr(u, 'title', None)
        if n: return n
    return None


async def name_from_seed(c, gid_names, seed_name):
    """لایهٔ ج: با slug تلگرام (مثل PlushPepe-1) شناسهٔ گیفت پایه را می‌گیریم."""
    base = ''.join(ch for ch in seed_name if ch.isalnum())
    for n in (1, 2, 3, 5, 10):
        try:
            r = await pacer.call(c, functions.payments.GetUniqueStarGiftRequest(slug=f'{base}-{n}'))
            gift = getattr(r, 'gift', r)
            gid = getattr(gift, 'gift_id', None)
            if gid: return gid, getattr(gift, 'title', None) or seed_name
        except errors.FloodWaitError: raise
        except Exception: continue
    return None


async def all_gifts(c):
    """خروجی: [(gift_or_id_holder, name)] برای همهٔ کالکشن‌هایی که پیدا شد."""
    out, seen_ids = [], set()
    try:
        r = await pacer.call(c, functions.payments.GetStarGiftsRequest(hash=0))
        gifts = list(getattr(r, 'gifts', None) or [])
        log(f'GetStarGifts → {type(r).__name__}، تعداد={len(gifts)}' + (f'، نمونه={gifts[0].__class__.__name__} فیلدها={keys_of(gifts[0])}' if gifts else ''))
        for g in gifts:
            name = getattr(g, 'title', None) or _names.get(g.id)
            if not name and getattr(g, 'availability_resale', 1) != 0:     # لایهٔ ب: نام از آگهی ریسِیل
                try: name = await name_from_resale(c, g.id)
                except errors.FloodWaitError: raise
                except Exception as e: log('name probe FAIL', g.id, repr(e)[:100])
            if name: _names[g.id] = name; out.append((g, name)); seen_ids.add(g.id)
        log(f'لایهٔ الف/ب: {len(out)} کالکشن با نام از {len(gifts)} گیفت')
    except Exception as e: log('GetStarGifts FAIL:', repr(e)[:200])
    if len(out) < 5:                                                       # لایهٔ ج: فهرست داخلی + getUniqueStarGift
        log('لایهٔ ج: پیدا کردن شناسه‌ها از روی فهرست داخلی و slug …')
        have = {n.lower() for _, n in out}
        class Holder:                                                      # مثل gift، فقط id و availability نامعلوم
            def __init__(self, i): self.id = i
        for sn in SEED:
            if left() < 60: break
            if sn.lower() in have: continue
            try: hit = await name_from_seed(c, None, sn)
            except Exception as e: log('seed FAIL', sn, repr(e)[:80]); continue
            if hit and hit[0] not in seen_ids:
                _names[hit[0]] = hit[1]; seen_ids.add(hit[0]); out.append((Holder(hit[0]), hit[1]))
        log(f'لایهٔ ج: مجموعاً {len(out)} کالکشن')
    return out


async def send_cols(c, gifts):
    items = [[name, int(getattr(g, 'availability_resale', 0) or 0)] for g, name in gifts]
    for msg in pack(items, lambda l, i, last: '/cols ' + json.dumps({'l': l}, ensure_ascii=False, separators=(',', ':'))):
        await to_bot(c, msg)
    log(f'کاتالوگ: {len(items)} کالکشن به بات فرستاده شد')


async def send_catalog(c, g, name):
    res = await resale(c, g.id, limit=1, attributes_hash=0)
    by = {'m': [], 'b': [], 's': []}
    for a in (getattr(res, 'attributes', None) or []):
        k, n = attr_kind(a), getattr(a, 'name', None)
        if k and n: by[k].append([n, permille(a)])
    total = 0
    for k, items in by.items():
        if not items: continue
        for msg in pack(items, lambda l, i, last, k=k: '/cat ' + json.dumps({'c': name, 'k': k, 'p': i, 'l': l}, ensure_ascii=False, separators=(',', ':'))):
            await to_bot(c, msg)
        total += len(items)
    return total


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


async def poll_collection(c, g, name):
    res = await resale(c, g.id, limit=PER_COL, sort_by_price=True)
    got = list(getattr(res, 'gifts', None) or [])
    if got and 'sample' not in _diag_done:
        _diag_done.add('sample'); log(f'نمونهٔ آگهی {name}: {got[0].__class__.__name__} فیلدها={keys_of(got[0])}')
    s = snaps.setdefault(name, Snap())
    now_ms, rows = int(time.time() * 1000), []
    first_poll = s.sig is None
    for u in got:
        p, num = stars_price(u), getattr(u, 'num', None)
        if not p or not isinstance(num, int): continue
        prev = s.first.get(num)
        if prev is None or p < prev[0]: s.first[num] = (p, 0 if first_poll else now_ms)   # آگهی تازه یا کاهش قیمت
        r = row(u, s.first[num][1])
        if r: rows.append(r)
    if got and not rows:
        warn_once('noprice', f'{name}: آگهی هست ولی قیمت استارز استخراج نشد (احتمالاً فقط TON). resell_amount={getattr(got[0], "resell_amount", None)!r:.200}')
    sig = tuple(sorted((r[0], r[1]) for r in rows))
    changed = sig != s.sig
    if not changed and time.time() - s.sent_at < RESEND_S: return 0
    for msg in ingest_msgs(name, rows): await to_bot(c, msg)
    s.sig, s.sent_at = sig, time.time()
    return 1 if changed else 0


# ───────────────────────── نرخ‌ها و توکن‌ها ─────────────────────────
def init_data(url: str) -> str:
    return (parse_qs(urlparse(url).fragment).get('tgWebAppData') or [''])[0]


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


TON_SOURCES = [   # صرافی‌هایی که از IP گیت‌هاب بسته‌اند (okx/bybit/binance/gate/kucoin) حذف شدند
    ('coingecko', 'https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd', lambda j: j['the-open-network']['usd']),
    ('coinbase', 'https://api.coinbase.com/v2/prices/TON-USD/spot', lambda j: j['data']['amount']),
    ('mexc', 'https://api.mexc.com/api/v3/ticker/price?symbol=TONUSDT', lambda j: j['price']),
    ('cryptocompare', 'https://min-api.cryptocompare.com/data/price?fsym=TON&tsyms=USD', lambda j: j['USD']),
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
    if not (API_ID and API_HASH and SESSION and BOT):
        log('❌ یکی از Secretها خالی است (TG_API_ID, TG_API_HASH, TG_SESSION, BOT_USERNAME)'); sys.exit(2)
    try:
        import telethon
        from telethon.tl.alltlobjects import LAYER
        log(f'telethon {telethon.__version__} layer {LAYER}')
    except Exception: pass
    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as c:
        try: me = await c.get_me(); log(f'وارد شد: id={me.id} (حساب {"bot!" if me.bot else "کاربر"})')
        except (errors.AuthKeyUnregisteredError, errors.AuthKeyDuplicatedError, errors.SessionRevokedError) as e:
            log('❌ سشن باطل شده؛ دوباره make_session.py بزن و TG_SESSION را عوض کن:', type(e).__name__); sys.exit(3)
        last = {'rates': 0.0, 'tokens': 0.0, 'cols': 0.0}
        gifts, cat_at, cycle = [], {}, 0
        due = lambda k, every: time.time() - last[k] >= every
        while left() > 20:
            cs = time.time(); cycle += 1
            try:
                if due('rates', 300): await push_rates(c); last['rates'] = time.time()
                if due('tokens', 1800): await refresh_tokens(c); last['tokens'] = time.time()
                if due('cols', 1800 if gifts else 300):
                    gifts = await all_gifts(c); last['cols'] = time.time()
                    if gifts: await send_cols(c, gifts)
                    else: log('❌ هیچ کالکشنی پیدا نشد؛ خط‌های بالا (GetStarGifts →) را برای پشتیبانی بفرست')
            except (errors.AuthKeyUnregisteredError, errors.AuthKeyDuplicatedError, errors.SessionRevokedError) as e:
                log('❌ سشن باطل شد:', type(e).__name__); sys.exit(3)
            except Exception as e: log('setup FAIL:', repr(e)[:200])
            has_counts = any(hasattr(g, 'availability_resale') for g, _ in gifts)
            live = [(g, n) for g, n in gifts if not has_counts or (getattr(g, 'availability_resale', 0) or 0) > 0 or cycle % 3 == 0]
            changed = 0
            for g, name in live:
                if left() < 20: break
                try: changed += await poll_collection(c, g, name)
                except Exception as e: log('poll FAIL', name, repr(e)[:160])
            todo = [(g, n) for g, n in gifts if time.time() - cat_at.get(n, 0) > CAT_EVERY_S][:CAT_PER_CYCLE]
            for g, name in todo:
                if left() < 20: break
                try: k = await send_catalog(c, g, name); cat_at[name] = time.time(); log(f'cat {name}: {k} ویژگی')
                except Exception as e: log('cat FAIL', name, repr(e)[:160]); cat_at[name] = time.time() - CAT_EVERY_S + 900
            log(f'چرخهٔ {cycle}: {len(live)} کالکشن، {changed} تغییر، {time.time() - cs:.0f}s')
            await asyncio.sleep(max(0, MIN_CYCLE_S - (time.time() - cs)))


if __name__ == '__main__':
    asyncio.run(main())
