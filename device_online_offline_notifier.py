import os
import mysql.connector
import requests
import smtplib
import pytz
import traceback

from datetime import datetime, date, time as dt_time, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"

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

OFFLINE_THRESHOLD = 5
SECOND_NOTIFICATION_HOURS = 6

BREVO_API_KEY = os.getenv("BREVO_API_KEY")
EMAIL_USER = "fertisenseiot@gmail.com"

IST_PYTZ = pytz.timezone("Asia/Kolkata")
UTC = timezone.utc

# ================== HELPERS ==================
def log(msg):
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")


def build_message(ntf_typ, devnm):
    messages = {
        3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")


# ---------------- send sms ----------------
def send_sms(phone, message):
    if not phone:
        return False
    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": phone,
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }
        r = requests.get(SMS_API_URL, params=params, timeout=30)
        log(f"SMS API -> {phone} status={r.status_code}")
        return r.status_code == 200
    except Exception as e:
        log(f"❌ SMS failed for {phone}: {e}")
        return False


# ---------------- Email templates ----------------
def offline_html(dev_name, diff_minutes=None):
    uptime_text = ""
    if diff_minutes is not None:
        uptime_text = f"<p><strong>Last seen:</strong> {diff_minutes:.1f} minutes ago</p>"
    return f"""
    <html><body>
    <h2 style="color:#b02a2a">⚠ {dev_name} is OFFLINE</h2>
    {uptime_text}
    <p>Please take necessary action.</p>
    <hr><small>Regards,<br/>Fertisense LLP</small>
    </body></html>
    """


def online_html(dev_name):
    return f"""
    <html><body>
    <h2 style="color:#2a9d2a">✔ {dev_name} is BACK ONLINE</h2>
    <p>No action required.</p>
    <hr><small>Regards,<br/>Fertisense LLP</small>
    </body></html>
    """


# ================== EMAIL ==================
def send_email(subject, html_content, email_ids):
    if not email_ids:
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(email_ids)
        msg.attach(MIMEText(html_content, "html"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, email_ids, msg.as_string())
        server.quit()
        log(f"✅ Email sent to {len(email_ids)} recipients")
        return True
    except Exception as e:
        log(f"❌ Email failed: {e}")
        return False


# ================== CONTACT FETCH ==================
def get_contact_info(device_id):
    conn = cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM iot_api_masterdevice WHERE DEVICE_ID=%s", (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], [], 1, 1

        org_id = device.get("ORGANIZATION_ID") or 1
        centre_id = device.get("CENTRE_ID") or 1

        cursor.execute("""
            SELECT u.PHONE, u.EMAIL, u.SEND_SMS, u.SEND_EMAIL
            FROM userorganizationcentrelink l
            JOIN master_user u ON l.USER_ID_id = u.USER_ID
            WHERE l.ORGANIZATION_ID_id=%s AND l.CENTRE_ID_id=%s
        """, (org_id, centre_id))
        users = cursor.fetchall()

        phones = []
        emails = []

        for u in users:
            if u.get("SEND_SMS") == 1 and u.get("PHONE"):
                phones.append(str(u["PHONE"]).strip())
            if u.get("SEND_EMAIL") == 1 and u.get("EMAIL"):
                emails.append(u["EMAIL"].strip())

        return list(set(phones)), list(set(emails)), org_id, centre_id

    except Exception as e:
        log(f"❌ Contact fetch error: {e}")
        return [], [], 1, 1
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ================== TIME PARSER ==================
def parse_reading_time(val):
    if isinstance(val, timedelta):
        s = int(val.total_seconds())
        return dt_time(s // 3600, (s % 3600) // 60, s % 60)
    if isinstance(val, dt_time):
        return val
    if isinstance(val, str):
        try:
            h, m, *s = map(int, val.split(":"))
            return dt_time(h, m, s[0] if s else 0)
        except:
            return None
    return None


# ================== MAIN LOGIC ==================
def check_device_online_status():
    conn = cursor = None
    try:
        log("🚀 Device online/offline check started")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        now = datetime.now(IST_PYTZ)

        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice WHERE DEVICE_STATUS=1")
        devices = cursor.fetchall()

        for d in devices:
            devid = d["DEVICE_ID"]
            devnm = d.get("DEVICE_NAME") or f"Device-{devid}"

            phones, emails, _, _ = get_contact_info(devid)

            cursor.execute("""
                SELECT READING_DATE, READING_TIME
                FROM device_reading_log
                WHERE DEVICE_ID=%s
                ORDER BY READING_DATE DESC, READING_TIME DESC
                LIMIT 1
            """, (devid,))
            last_read = cursor.fetchone()

            if last_read:
                rd = last_read["READING_DATE"]
                rt = parse_reading_time(last_read["READING_TIME"])
                if rd and rt:
                    last_update = datetime.combine(rd, rt).replace(tzinfo=UTC).astimezone(IST_PYTZ)
                else:
                    last_update = None
            else:
                last_update = None

            diff_minutes = (now - last_update).total_seconds() / 60 if last_update else OFFLINE_THRESHOLD + 1
            current_state = 1 if diff_minutes <= OFFLINE_THRESHOLD else 0

            cursor.execute("""
                SELECT * FROM device_status_alarm_log
                WHERE DEVICE_ID=%s AND IS_ACTIVE=1
                ORDER BY DEVICE_STATUS_ALARM_ID DESC LIMIT 1
            """, (devid,))
            alarm = cursor.fetchone()

            # ---------- ONLINE ----------
            if current_state == 1:
                if alarm:
                    message = build_message(5, devnm)
                    html = online_html(devnm)

                    sms_ok = any(send_sms(p, message) for p in phones)
                    email_ok = send_email(f"{devnm} Back Online", html, emails)

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
                        now.date() if sms_ok else alarm["SMS_DATE"],
                        now.time() if sms_ok else alarm["SMS_TIME"],
                        now.date() if email_ok else alarm["EMAIL_DATE"],
                        now.time() if email_ok else alarm["EMAIL_TIME"],
                        alarm["DEVICE_STATUS_ALARM_ID"]
                    ))
                    conn.commit()
                continue

            # ---------- OFFLINE ----------
            if not alarm:
                message = build_message(3, devnm)
                html = offline_html(devnm, diff_minutes)

                sms_ok = any(send_sms(p, message) for p in phones)
                email_ok = send_email(f"{devnm} Offline", html, emails)

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
