import asyncio, os
import qrcode
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

api_id = int(os.environ.get('TG_API_ID') or input('API_ID: '))
api_hash = os.environ.get('TG_API_HASH') or input('API_HASH: ')
proxy = {'proxy_type': 'socks5', 'addr': '127.0.0.1', 'port': 10808}

def show(url):
    qrcode.make(url).save('qr.png')
    try: os.startfile('qr.png')
    except Exception: print('فایل qr.png کنار اسکریپت ساخته شد؛ بازش کن.')

async def main():
    c = TelegramClient(StringSession(), api_id, api_hash, proxy=proxy)
    await c.connect()
    qr = await c.qr_login()
    show(qr.url)
    print('QR باز شد. با گوشی اسکن کن (Settings ← Devices ← Link Desktop Device).')
    while True:
        try:
            await qr.wait(25)
            break
        except asyncio.TimeoutError:
            await qr.recreate()
            show(qr.url)
            print('QR تازه شد؛ پنجرهٔ جدید رو اسکن کن.')
        except SessionPasswordNeededError:
            await c.sign_in(password=input('رمز دو مرحله‌ای: '))
            break
    print('\n=== TG_SESSION ===\n' + c.session.save() + '\n==================')
    try: os.remove('qr.png')
    except Exception: pass

asyncio.run(main())