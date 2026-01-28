import os
import mysql.connector
import requests
import pytz
import traceback
from datetime import datetime, time as dt_time, timedelta, timezone
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException


# ================== ENV ==================
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

# ================== GLOBAL CACHE ==================
CONTACT_CACHE = {}

# ================== CONFIG ==================
db_config = {
    "host": "switchyard.proxy.rlwy.net",
    "user": "root",
    "port": 28085,
    "password": "NOtYUNawwodSrBfGubHhwKaFtWyGXQct",
    "database": "railway",
}

SMS_API_URL = "https://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

OFFLINE_THRESHOLD = 5  # minutes

EMAIL_USER = "fertisenseiot@gmail.com"

IST = pytz.timezone("Asia/Kolkata")
UTC = timezone.utc


# ================== HELPERS ==================
def log(msg):
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")


def build_message(ntf_typ, devnm):
    if ntf_typ == 3:
        # return f"WARNING!! The {devnm} is offline. Please take necessary action- Regards Fertisense LLP"
        return f"WARNING!! The  {devnm} is offline. Please take necessary action- Regards Fertisense LLP"
    if ntf_typ == 5:
        #return f"INFO!! The {devnm} is back online. No action is required - Regards Fertisense LLP"
        return f"INFO!! The  {devnm} is back online. No action is required - Regards Fertisense LLP"

    return f"Alert for {devnm}- Regards Fertisense LLP"


# ================== SMS ==================
def send_sms_single(phone, message):
    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": str(phone).strip(),
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }
        r = requests.get(SMS_API_URL, params=params, timeout=30)
        log(f"SMS -> {phone} | HTTP={r.status_code}")
        return r.status_code == 200
    except Exception as e:
        log(f"❌ SMS error: {e}")
        return False


def send_sms(phones, message):
    sent = False
    for p in phones:
        if send_sms_single(p, message):
            sent = True
    return sent


# ================== EMAIL ==================
def send_email(subject, html_content, email_ids):
    if not email_ids:
        return False
    try:
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = BREVO_API_KEY
        api = sib_api_v3_sdk.TransactionalEmailsApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )
        api.send_transac_email(
            sib_api_v3_sdk.SendSmtpEmail(
                subject=subject,
                html_content=html_content,
                sender={"name": "Fertisense", "email": EMAIL_USER},
                to=[{"email": e} for e in email_ids]
            )
        )
        log(f"📧 Email sent -> {email_ids}")
        return True
    except ApiException as e:
        log(f"❌ Email error: {e}")
        return False


def offline_html(dev_name, diff):
    return f"""
    <html><body>
    <h2 style="color:#b02a2a">⚠ {dev_name} is OFFLINE</h2>
    <p>Last seen {diff:.1f} minutes ago</p>
    <hr>Regards<br/>Fertisense LLP
    </body></html>
    """


def online_html(dev_name):
    return f"""
    <html><body>
    <h2 style="color:#2a9d2a">✔ {dev_name} is BACK ONLINE</h2>
    <p>No action required.</p>
    <hr>Regards<br/>Fertisense LLP
    </body></html>
    """


# ================== CONTACT FETCH ==================
def get_contact_info(cursor, device_id):
    if device_id in CONTACT_CACHE:
        return CONTACT_CACHE[device_id]

    cursor.execute("""
        SELECT u.PHONE, u.EMAIL
        FROM userorganizationcentrelink l
        JOIN master_user u ON l.USER_ID_id = u.USER_ID
        JOIN iot_api_masterdevice d ON d.ORGANIZATION_ID=l.ORGANIZATION_ID_id
        WHERE d.DEVICE_ID=%s
    """, (device_id,))

    phones, emails = [], []
    for r in cursor.fetchall():
        if r["PHONE"]:
            phones.extend(p.strip() for p in r["PHONE"].replace("/", ",").split(","))
        if r["EMAIL"]:
            emails.append(r["EMAIL"].strip())

    phones = list(dict.fromkeys(phones))
    emails = list(dict.fromkeys(emails))

    CONTACT_CACHE[device_id] = (phones, emails)
    return phones, emails


# ================== TIME PARSER ==================
def parse_reading_time(val):
    if isinstance(val, dt_time):
        return val
    if isinstance(val, timedelta):
        s = int(val.total_seconds())
        return dt_time(s // 3600, (s % 3600) // 60, s % 60)
    if isinstance(val, str):
        h, m, *s = map(int, val.split(":"))
        return dt_time(h, m, s[0] if s else 0)
    return None


# ================== MAIN ==================
def check_device_online_status():
    conn = cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        now = datetime.now(IST)

        # ================= PASS 1 : OFFLINE =================
        log("🔴 PASS 1 : OFFLINE CHECK")

        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice")
        devices = cursor.fetchall()

        for d in devices:
            devid = d["DEVICE_ID"]
            devnm = d.get("DEVICE_NAME") or f"Device-{devid}"
            phones, emails = get_contact_info(cursor, devid)

            cursor.execute("""
                SELECT READING_DATE, READING_TIME
                FROM device_reading_log
                WHERE DEVICE_ID=%s
                ORDER BY READING_DATE DESC, READING_TIME DESC
                LIMIT 1
            """, (devid,))
            r = cursor.fetchone()

            last_update = None
            if r:
                rt = parse_reading_time(r["READING_TIME"])
                if rt:
                    last_update = datetime.combine(
                        r["READING_DATE"], rt
                    ).replace(tzinfo=UTC).astimezone(IST)

            diff = (now - last_update).total_seconds() / 60 if last_update else 999

            if diff > OFFLINE_THRESHOLD:
                cursor.execute("""
                    SELECT 1 FROM device_status_alarm_log
                    WHERE DEVICE_ID=%s AND IS_ACTIVE=1
                """, (devid,))
                if not cursor.fetchone():
                    log(f"🔴 OFFLINE -> {devnm}")
                    sms_ok = send_sms(phones, build_message(3, devnm))
                    email_ok = send_email(
                        f"{devnm} Offline",
                        offline_html(devnm, diff),
                        emails
                    )

                    cursor.execute("""
                        INSERT INTO device_status_alarm_log
                        (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE,
                         CREATED_ON_DATE, CREATED_ON_TIME,
                         SMS_DATE, SMS_TIME, EMAIL_DATE, EMAIL_TIME)
                        VALUES (%s,1,1,%s,%s,%s,%s,%s,%s)
                    """, (
                        devid,
                        now.date(), now.time(),
                        now.date() if sms_ok else None,
                        now.time() if sms_ok else None,
                        now.date() if email_ok else None,
                        now.time() if email_ok else None
                    ))
                    conn.commit()

        # ================= PASS 2 : BACK ONLINE =================
        log("🟢 PASS 2 : BACK ONLINE CHECK")

        cursor.execute("""
            SELECT DEVICE_STATUS_ALARM_ID, DEVICE_ID
            FROM device_status_alarm_log
            WHERE IS_ACTIVE=1
        """)
        alarms = cursor.fetchall()

        for a in alarms:
            devid = a["DEVICE_ID"]
            alarm_id = a["DEVICE_STATUS_ALARM_ID"]

            cursor.execute(
                "SELECT DEVICE_NAME FROM iot_api_masterdevice WHERE DEVICE_ID=%s",
                (devid,)
            )
            devnm = cursor.fetchone().get("DEVICE_NAME") or f"Device-{devid}"

            phones, emails = get_contact_info(cursor, devid)

            cursor.execute("""
                SELECT READING_DATE, READING_TIME
                FROM device_reading_log
                WHERE DEVICE_ID=%s
                ORDER BY READING_DATE DESC, READING_TIME DESC
                LIMIT 1
            """, (devid,))
            r = cursor.fetchone()
            if not r:
                continue

            rt = parse_reading_time(r["READING_TIME"])
            if not rt:
                continue

            last_update = datetime.combine(
                r["READING_DATE"], rt
            ).replace(tzinfo=UTC).astimezone(IST)

            diff = (now - last_update).total_seconds() / 60

            if diff <= OFFLINE_THRESHOLD:
                log(f"🟢 BACK ONLINE -> {devnm}")
                sms_ok = send_sms(phones, build_message(5, devnm))
                email_ok = send_email(
                    f"{devnm} Back Online",
                    online_html(devnm),
                    emails
                )

                cursor.execute("""
                    UPDATE device_status_alarm_log
                    SET IS_ACTIVE=0,
                        UPDATED_ON_DATE=%s,
                        UPDATED_ON_TIME=%s,
                        SMS_DATE=%s,
                        SMS_TIME=%s,
                        EMAIL_DATE=%s,
                        EMAIL_TIME=%s
                    WHERE DEVICE_STATUS_ALARM_ID=%s
                """, (
                    now.date(), now.time(),
                    now.date() if sms_ok else None,
                    now.time() if sms_ok else None,
                    now.date() if email_ok else None,
                    now.time() if email_ok else None,
                    alarm_id
                ))
                conn.commit()

        log("✅ Device check completed")

    except Exception as e:
        log(f"❌ Fatal error: {e}")
        traceback.print_exc()

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ================== RUN ==================
if __name__ == "__main__":
    check_device_online_status()
